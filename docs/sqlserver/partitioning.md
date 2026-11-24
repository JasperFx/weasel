# Table Partitioning

PostgreSQL partitions a table into child tables. SQL Server does it differently, with two server objects:

* a **partition function** that defines the boundary values and the direction (`RANGE LEFT` / `RANGE RIGHT`), and
* a **partition scheme** that maps the function's partitions onto filegroups.

The table is then created *on* the partition scheme. Weasel models this through the
`ISqlServerPartitioning` strategy on `Table`, with `RangePartitioning` for declarative date/numeric
ranges, `ManagedRangePartitions` for rolling time windows, and `ManagedTenantPartitions` for runtime,
per-tenant partitioning.

## Range Partitioning

Use `PartitionByRange(column, sqlDataType)` and add the boundary values. The boundaries are the points at
which a new partition begins; `N` boundaries produce `N + 1` partitions. The default direction is
`RANGE RIGHT`, which is the natural choice for time-series data (each boundary is the *inclusive lower
bound* of the next partition).

<!-- snippet: sample_ss_range_partitioning -->
<a id='snippet-sample_ss_range_partitioning'></a>
```cs
var table = new Table("metrics.metrics_sample");
table.AddColumn<int>("id");
table.AddColumn("bucket_end", "datetime").NotNull();
table.AddColumn("data", "nvarchar(max)");

// On SQL Server the partition column MUST participate in the primary key
table.ModifyColumn("id").AsPrimaryKey();
table.ModifyColumn("bucket_end").AsPrimaryKey();

var partitioning = table.PartitionByRange("bucket_end", "datetime");
partitioning.AddBoundary(new DateTime(2026, 1, 1));
partitioning.AddBoundary(new DateTime(2026, 2, 1));
partitioning.AddBoundary(new DateTime(2026, 3, 1));
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerPartitioningSamples.cs#L17-L31' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_range_partitioning' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This generates a `CREATE PARTITION FUNCTION`, a `CREATE PARTITION SCHEME`, and a `CREATE TABLE ... ON` the
scheme. Leaving out the preamble that refreshes an unused function and scheme, it looks like this:

```sql
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_metrics_sample_bucket_end')
    CREATE PARTITION FUNCTION [pf_metrics_sample_bucket_end] (datetime) AS RANGE RIGHT FOR VALUES ('2026-01-01 00:00:00', '2026-02-01 00:00:00', '2026-03-01 00:00:00');
IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_metrics_sample_bucket_end')
    CREATE PARTITION SCHEME [ps_metrics_sample_bucket_end] AS PARTITION [pf_metrics_sample_bucket_end] ALL TO ([PRIMARY]);
IF OBJECT_ID(N'metrics.metrics_sample', N'U') IS NULL
BEGIN
CREATE TABLE metrics.metrics_sample (
    id            int              NOT NULL,
    bucket_end    datetime         NOT NULL,
    data          nvarchar(max)    NULL,
CONSTRAINT pkey_metrics_sample_id_bucket_end PRIMARY KEY (id, bucket_end)
) ON [ps_metrics_sample_bucket_end]([bucket_end]);
END
```

::: tip
Add boundaries with the strongly typed `AddBoundary<T>(value)` overload (e.g. a `DateTime`,
`DateTimeOffset`, or `int`) rather than a raw SQL string. Weasel formats the value into a canonical SQL
literal, which is what lets schema migration round-trip the boundaries back from the database without
reporting a spurious change. See [Schema Migrations](#schema-migrations) below.
:::

`RangePartitioning` exposes a few knobs:

| Member | Purpose |
| --- | --- |
| `IsRangeRight` | `true` (default) for `RANGE RIGHT`, `false` for `RANGE LEFT`. |
| `Filegroup` | The filegroup all partitions are mapped to. Defaults to `PRIMARY`. |
| `AddBoundary<T>(value)` | Add a typed boundary, formatted to a canonical SQL literal. |
| `AddBoundary(string)` | Add a boundary as a raw SQL literal (advanced; opts out of round-trip matching). |

## Schema Migrations

`RangePartitioning` participates in Weasel's normal schema-diff machinery. When you fetch the existing
table back from the database, Weasel reads the partition function, scheme, column, type, direction, and
boundary values into `Table.PartitionInfo`, then compares them against what you declared:

<!-- snippet: sample_ss_partitioning_delta -->
<a id='snippet-sample_ss_partitioning_delta'></a>
```cs
var delta = await table.FindDeltaAsync(connection);

// SchemaPatchDifference.None    -> the table matches the database
// SchemaPatchDifference.Update  -> new boundaries can be rolled forward in place
// SchemaPatchDifference.Invalid -> the partitioning must be rebuilt
Console.WriteLine(delta.PartitioningDifference);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerPartitioningSamples.cs#L36-L43' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_partitioning_delta' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

### Rolling new partitions forward

Adding boundaries beyond what already exists is an **additive** change. Weasel migrates it in place with
`ALTER PARTITION FUNCTION ... SPLIT RANGE` (preceded by `ALTER PARTITION SCHEME ... NEXT USED`), so no
table rebuild or data movement is required:

<!-- snippet: sample_ss_roll_forward_boundaries -->
<a id='snippet-sample_ss_roll_forward_boundaries'></a>
```cs
// The database already has boundaries for Jan/Feb 2026. Declare March as well...
var partitioning = table.PartitionByRange("bucket_end", "datetime");
partitioning.AddBoundary(new DateTime(2026, 1, 1));
partitioning.AddBoundary(new DateTime(2026, 2, 1));
partitioning.AddBoundary(new DateTime(2026, 3, 1)); // new

// ...and Weasel emits: ALTER PARTITION FUNCTION [...]() SPLIT RANGE ('2026-03-01 00:00:00');
await table.ApplyChangesAsync(connection);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerPartitioningSamples.cs#L48-L57' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_roll_forward_boundaries' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This is the recommended way to "roll forward" a time-series table month over month: keep extending the
declared boundary list and let migration add the new partitions.

Each split is guarded by a lookup in `sys.partition_range_values`, and the `MERGE RANGE` that rolls it
back is guarded the same way, so a rendered migration script can be run more than once. The boundary is
cast to the partition function's type before it is compared, because the catalog stores it as a
`sql_variant`:

```sql
IF NOT EXISTS (SELECT 1 FROM sys.partition_range_values rv INNER JOIN sys.partition_functions pf ON pf.function_id = rv.function_id WHERE pf.name = 'pf_metrics_sample_bucket_end' AND rv.value = CAST('2026-03-01 00:00:00' AS datetime))
BEGIN
    ALTER PARTITION SCHEME [ps_metrics_sample_bucket_end] NEXT USED [PRIMARY];
    ALTER PARTITION FUNCTION [pf_metrics_sample_bucket_end]() SPLIT RANGE ('2026-03-01 00:00:00');
END
```

### Partitioning a table that already has rows

If you turn RANGE partitioning on for a table that already exists, unpartitioned and full of rows,
Weasel moves the table onto the partition scheme where it is. It rebuilds the clustered primary key on
the scheme, which carries every row with it, so nothing is dropped and nothing is copied into another
table:

<!-- snippet: sample_ss_partition_an_existing_table -->
<a id='snippet-sample_ss_partition_an_existing_table'></a>
```cs
// dbo.events already exists, unpartitioned, with seq_id as its primary key
var table = new Table("dbo.events");
table.AddColumn("seq_id", "bigint").AsPrimaryKey().AutoNumber();
table.AddColumn("data", "varchar(100)").NotNull();

// The partition column joins the clustered primary key...
table.AddColumn("is_archived", "bit").NotNull().DefaultValue(0).AsPrimaryKey();

// ...and the table goes onto a RANGE scheme
table.PartitionByRange("is_archived", "bit").AddBoundary(1);

// The rows move onto the scheme where they are. Nothing is dropped or copied.
await table.ApplyChangesAsync(connection);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerPartitioningSamples.cs#L62-L76' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_partition_an_existing_table' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

SQL Server partitions a table through its clustered index, so this only works when the primary key is
clustered and includes the partition column. Foreign keys on other tables that reference the primary key
come off first, since SQL Server won't drop a key anything still references, and the ones those tables
still declare go back on afterwards. Indexes the migration creates are built after the move, so they are
aligned with the scheme. The rendered script is guarded the same way as the rest of Weasel's scripts, so a
move that was cut off part way finishes on the next run, and the rollback moves the table back off the
scheme with its rows.

The delta is still reported as `SchemaPatchDifference.Invalid`, because it can't be expressed as an
`ALTER`. `AutoCreate.CreateOrUpdate` and `AutoCreate.All` both apply it in place, while
`AutoCreate.CreateOnly` refuses it.

::: warning
Moving a large table onto a scheme rebuilds its clustered index, which takes a schema modification lock
and moves every row. Run it in a maintenance window, and look at the script from `db-patch` first.
:::

Going the other way, a model that stops partitioning a table and changes its primary key moves the table
back off the scheme onto the default filegroup, again keeping its rows.

### Rebuilds

Changing the partition **column**, **data type** or **direction** of a table that is already
partitioned, or *removing* a boundary that exists in the database, cannot be done with an in-place
`SPLIT`. Weasel reports these as `SchemaPatchDifference.Invalid` rather than silently rebuilding the table
and moving every row. Trimming aged partitions for data retention is a runtime operation rather than a
declarative schema change. See [Rolling time windows](#rolling-time-windows), which owns both halves for
you.

## Rolling Time Windows

Extending a declared boundary list by hand works, but it is not what a time-series table actually wants.
The window *moves*: every period a new partition is needed at the leading edge, and the period that has
fallen out of the retention policy at the trailing edge should be reclaimed, cheaply, by deallocating its
pages rather than by a mass `DELETE`. Doing that on SQL Server is otherwise a hand-rolled T-SQL chore; the
published recipes amount to "write a script that sets `NEXT USED` and `SPLIT`s, and another that
`MERGE`s".

`ManagedRangePartitions` owns both halves. Declare intent -- a period, how many periods to provision
ahead, how many to retain behind -- and Weasel writes every statement:

<!-- snippet: sample_ss_rolling_window_partitioning -->
<a id='snippet-sample_ss_rolling_window_partitioning'></a>
```cs
var manager = new ManagedRangePartitions(
    RollingWindowPolicy.Monthly(periodsAhead: 3, periodsBehind: 6),
    column: "occurred_at");

var table = new Table("dbo.metrics");
table.AddColumn<int>("id");
table.AddColumn("occurred_at", "datetime2").NotNull();
table.AddColumn("value", "float");

// On SQL Server the partition column MUST participate in the primary key
table.ModifyColumn("id").AsPrimaryKey();
table.ModifyColumn("occurred_at").AsPrimaryKey();

table.PartitionByRollingWindow(manager);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerPartitioningSamples.cs#L81-L96' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_rolling_window_partitioning' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`RollingWindowPolicy` supports `Hourly`, `Daily`, `Weekly`, `Monthly`, and `Yearly` windows, and all
boundary arithmetic is done in UTC. The strategy is `RANGE RIGHT`, so each boundary is the inclusive
lower bound of its period. The column type defaults to `datetime2`; pass `sqlDataType` to the
`ManagedRangePartitions` constructor to use another.

The boundaries are the window's period starts **plus** the exclusive end of the newest provisioned
period. That trailing boundary matters. Without it the top partition would be
`[newest period start, +infinity)` and would hold the newest period's rows, so the next roll-forward
would `SPLIT` a partition containing data, which is a fully logged row-movement operation. With it, the
top partition is always beyond everything provisioned and therefore empty, and every `SPLIT` stays
metadata-only.

At runtime:

<!-- snippet: sample_ss_rolling_window_apply -->
<a id='snippet-sample_ss_rolling_window_apply'></a>
```cs
// Split in any missing boundaries at the leading edge, then truncate and merge
// away everything below the retention floor. Idempotent, so this is safe on
// every startup and on a timer.
await manager.ApplyAsync(database, logger, token);

// ...or run just one half
await manager.RollForwardAsync(database, logger, token);
await manager.DropAgedPartitionsAsync(database, logger, token);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerPartitioningSamples.cs#L102-L111' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_rolling_window_apply' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Two properties make this different from extending a static boundary list:

- **Rolling forward is always additive, never a rebuild.** A boundary the declaration no longer names is
  a period that has aged out. That's the normal steady state of a rolling window, not drift, so
  `CreateDelta` reports `Additive` or `None`, never `Rebuild`, for a boundary-set difference. A column,
  type or direction change still rebuilds.
- **Retiring a period is `TRUNCATE` then `MERGE RANGE`, in that order.** `MERGE RANGE` on a partition
  that still holds rows does not reclaim anything. It *moves* those rows into the neighbouring
  partition, which is the opposite of the point. Truncating the partition first (`TRUNCATE TABLE ...
  WITH (PARTITIONS (n))`, SQL Server 2016+) deallocates its pages in O(1), and the merge that follows is
  then metadata-only against an empty partition. If the truncate fails, the boundary is deliberately
  left in place.

Retention only retires boundaries that land exactly on a period start for the policy, so a hand-added
boundary is left strictly alone. It also reclaims the partition below the oldest boundary once that
boundary ages out. That partition holds late-arriving rows dated before the window ever started, and it
is entirely below the retention floor.

::: warning
Retiring a period removes its rows. That's the point, since it is what makes reclaim O(1), but choose
`periodsBehind` to match the retention policy you actually want.
:::

## Managed Tenant Partitioning

For multi-tenant tables where each tenant gets its own physical partition, use
`PartitionByManagedTenants(...)` with a shared `ManagedTenantPartitions` manager. Tenants are allocated
integer ordinals at sign-up, the table is partitioned `RANGE RIGHT` on the ordinal column, and the
partition function is split for each new tenant at runtime. See the manager's API for
`AddPartitionToAllTables` / `DropPartitionFromAllTables`.

<!-- snippet: sample_ss_managed_tenant_partitions -->
<a id='snippet-sample_ss_managed_tenant_partitions'></a>
```cs
var manager = new ManagedTenantPartitions(
    "tenant-partitions",
    new DbObjectName("dbo", "tenant_partitions"));

table.PartitionByManagedTenants(manager);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerPartitioningSamples.cs#L116-L122' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_managed_tenant_partitions' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`ManagedTenantPartitions` owns its boundaries at runtime, so unlike `RangePartitioning` and
`ManagedRangePartitions`, its boundaries are not diffed or migrated by `FindDeltaAsync`. When a table
joins an *existing* managed set (tenants already registered), call
`MigrateAllTablesAsync(logger, database, token)` to back-fill every managed table with the full set of
registered tenant ordinals.

### Dropping tenants: data semantics

SQL Server's `MERGE RANGE` only removes the boundary point. The tenant's rows are merged into the
neighboring partition, not deleted. `DropPartitionFromAllTables` therefore takes a `TenantDropBehavior`:

- `TenantDropBehavior.RetainData` (default) merges only, and leaves any data purge to you.
- `TenantDropBehavior.DeleteData` deletes the tenant's rows from every managed table first, then merges.
  This matches the hard delete of the PostgreSQL managed drop (`DETACH PARTITION` + `DROP TABLE`).

With `DeleteData`, tables with a foreign key to another managed table are purged before the table they
reference, so the referenced rows are no longer in use when they are deleted. A self-referencing foreign
key doesn't matter. Tables whose foreign keys form a cycle can't be ordered: a `DELETE` that fails is
logged, and that table's boundary is left in place rather than merging the rows it still holds.

### Tenant bucketing (many tenants per partition)

By default every tenant gets its own ordinal, guarded by a unique index on the registry table. For
applications with many small tenants, or to stay clear of SQL Server's 15,000-partition-per-table
ceiling, set `AllowOrdinalSharing = true` (which removes the unique index) and assign ordinals
explicitly so several tenants share one partition:

<!-- snippet: sample_ss_tenant_bucketing -->
<a id='snippet-sample_ss_tenant_bucketing'></a>
```cs
manager.AllowOrdinalSharing = true;

var result = await manager.AddPartitionsToAllTables(logger, database,
    new Dictionary<string, int> { ["acme"] = 1, ["globex"] = 1, ["initech"] = 2 },
    token);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerPartitioningSamples.cs#L128-L134' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_tenant_bucketing' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A shared ordinal is only merged (and only purged under `DeleteData`) once its *last* tenant is
dropped. While other tenants still map to it, the partition and all of its rows are left alone.

### Batch registration and status reporting

The batch `AddPartitionsToAllTables(...)` overloads return a `TenantPartitionAddResult` carrying both
the assigned `tenant_id -> ordinal` map and a per-table `TablePartitionStatus[]`, the same as the
PostgreSQL `ManagedListPartitions` batch add. That lets you surface partial failures, since one table
failing to split no longer prevents the remaining tables from being split.

### A time limit for the SPLIT

A `SPLIT` waits for a schema modification lock on the table, and while it waits every later reader and
writer of that table queues behind it. To give up on the `SPLIT` sooner than the connection's command
timeout, set `SplitCommandTimeout`:

<!-- snippet: sample_ss_split_command_timeout -->
<a id='snippet-sample_ss_split_command_timeout'></a>
```cs
manager.SplitCommandTimeout = TimeSpan.FromSeconds(5);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerPartitioningSamples.cs#L139-L141' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_split_command_timeout' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

It applies to each `ALTER PARTITION FUNCTION ... SPLIT RANGE` and the `NEXT USED` before it, and to
nothing else: loading the registry and writing the tenant into it keep the connection's command timeout.
A `SPLIT` that gives up is reported as `Failed` for that table like any other failed split, the tenant
keeps its ordinal, and `MigrateAllTablesAsync` (or the next registration) fills the boundary in later.
The default, `null`, leaves the `SPLIT` on the connection's command timeout.

To set it for one call instead, pass it to `AddPartitionsToAllTables`:

<!-- snippet: sample_ss_split_command_timeout_for_one_call -->
<a id='snippet-sample_ss_split_command_timeout_for_one_call'></a>
```cs
var result = await manager.AddPartitionsToAllTables(logger, database, ["acme"],
    TimeSpan.FromSeconds(5), token);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerPartitioningSamples.cs#L147-L150' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_split_command_timeout_for_one_call' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
