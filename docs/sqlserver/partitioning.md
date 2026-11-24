# Table Partitioning

Unlike PostgreSQL — which partitions a table into child tables — SQL Server partitioning is built from two
server objects:

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

```cs
var table = new Table("metrics.metrics_sample");
table.AddColumn<int>("id");
table.AddColumn("bucket_end", "datetime").NotNull();
table.AddColumn("data", "nvarchar(max)");

// On SQL Server the partition column MUST participate in the primary key.
table.ModifyColumn("id").AsPrimaryKey();
table.ModifyColumn("bucket_end").AsPrimaryKey();

var partitioning = table.PartitionByRange("bucket_end", "datetime");
partitioning.AddBoundary(new DateTime(2026, 1, 1));
partitioning.AddBoundary(new DateTime(2026, 2, 1));
partitioning.AddBoundary(new DateTime(2026, 3, 1));
```

This generates a `CREATE PARTITION FUNCTION`, a `CREATE PARTITION SCHEME`, and a `CREATE TABLE ... ON` the
scheme:

```sql
CREATE PARTITION FUNCTION [pf_metrics_metrics_sample_bucket_end] (datetime)
    AS RANGE RIGHT FOR VALUES ('2026-01-01 00:00:00', '2026-02-01 00:00:00', '2026-03-01 00:00:00');
CREATE PARTITION SCHEME [ps_metrics_metrics_sample_bucket_end]
    AS PARTITION [pf_metrics_metrics_sample_bucket_end] ALL TO ([PRIMARY]);
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

```cs
var delta = await table.FindDeltaAsync(connection);

// SchemaPatchDifference.None    -> the table matches the database
// SchemaPatchDifference.Update  -> new boundaries can be rolled forward in place
// SchemaPatchDifference.Invalid -> the partitioning must be rebuilt
Console.WriteLine(delta.PartitioningDifference);
```

### Rolling new partitions forward

Adding boundaries beyond what already exists is an **additive** change. Weasel migrates it in place with
`ALTER PARTITION FUNCTION ... SPLIT RANGE` (preceded by `ALTER PARTITION SCHEME ... NEXT USED`), so no
table rebuild or data movement is required:

```cs
// The database already has boundaries for Jan/Feb 2026. Declare March as well...
var partitioning = table.PartitionByRange("bucket_end", "datetime");
partitioning.AddBoundary(new DateTime(2026, 1, 1));
partitioning.AddBoundary(new DateTime(2026, 2, 1));
partitioning.AddBoundary(new DateTime(2026, 3, 1)); // new

// ...and Weasel emits: ALTER PARTITION FUNCTION [...]() SPLIT RANGE ('2026-03-01 00:00:00');
await table.ApplyChangesAsync(connection);
```

This is the recommended way to "roll forward" a time-series table month over month: keep extending the
declared boundary list and let migration add the new partitions.

Each split is guarded by a lookup in `sys.partition_range_values`, and the `MERGE RANGE` that rolls it
back is guarded the same way, so a rendered migration script can be run more than once. The boundary is
cast to the partition function's type before it is compared, because the catalog stores it as a
`sql_variant`:

```sql
IF NOT EXISTS (SELECT 1 FROM sys.partition_range_values rv INNER JOIN sys.partition_functions pf ON pf.function_id = rv.function_id WHERE pf.name = 'pf_metrics_metrics_sample_bucket_end' AND rv.value = CAST('2026-03-01 00:00:00' AS datetime))
BEGIN
    ALTER PARTITION SCHEME [ps_metrics_metrics_sample_bucket_end] NEXT USED [PRIMARY];
    ALTER PARTITION FUNCTION [pf_metrics_metrics_sample_bucket_end]() SPLIT RANGE ('2026-03-01 00:00:00');
END
```

### Rebuilds

Changing the partition **column** or **data type**, or *removing* a boundary that exists in the database,
cannot be done with an in-place `SPLIT`. Weasel reports these as `SchemaPatchDifference.Invalid` rather
than silently rebuilding the table and moving every row. Trimming aged partitions for data retention is a
runtime operation rather than a declarative schema change — see
[Rolling time windows](#rolling-time-windows), which owns both halves for you.

## Rolling Time Windows

Extending a declared boundary list by hand works, but it is not what a time-series table actually wants.
The window *moves*: every period a new partition is needed at the leading edge, and the period that has
fallen out of the retention policy at the trailing edge should be reclaimed — cheaply, by deallocating
its pages rather than by a mass `DELETE`. Doing that on SQL Server is otherwise a hand-rolled T-SQL
chore; the published recipes amount to "write a script that sets `NEXT USED` and `SPLIT`s, and another
that `MERGE`s".

`ManagedRangePartitions` owns both halves. Declare intent — a period, how many periods to provision
ahead, how many to retain behind — and Weasel writes every statement:

```cs
var manager = new ManagedRangePartitions(
    RollingWindowPolicy.Monthly(periodsAhead: 3, periodsBehind: 6),
    column: "occurred_at");

var table = new Table("dbo.metrics");
table.AddColumn<int>("id");
table.AddColumn("occurred_at", "datetime2").NotNull();
table.AddColumn("value", "float");

// On SQL Server the partition column MUST participate in the primary key.
table.ModifyColumn("id").AsPrimaryKey();
table.ModifyColumn("occurred_at").AsPrimaryKey();

table.PartitionByRollingWindow(manager);
```

`RollingWindowPolicy` supports `Hourly`, `Daily`, `Weekly`, `Monthly`, and `Yearly` windows, and all
boundary arithmetic is done in UTC. The strategy is `RANGE RIGHT`, so each boundary is the inclusive
lower bound of its period.

The boundaries are the window's period starts **plus** the exclusive end of the newest provisioned
period. That trailing boundary matters: without it the top partition would be
`[newest period start, +infinity)` and would hold the newest period's rows, so the next roll-forward
would `SPLIT` a partition containing data — a fully logged row-movement operation. With it, the top
partition is always beyond everything provisioned and therefore empty, and every `SPLIT` stays
metadata-only.

At runtime:

```cs
// Split in any missing boundaries at the leading edge, then truncate and merge
// away everything below the retention floor. Idempotent, so this is safe on
// every startup and on a timer.
await manager.ApplyAsync(database, logger, token);

// ...or run just one half
await manager.RollForwardAsync(database, logger, token);
await manager.DropAgedPartitionsAsync(database, logger, token);
```

Two properties make this different from extending a static boundary list:

- **Rolling forward is always additive, never a rebuild.** A boundary the declaration no longer names is
  a period that has aged out — the normal steady state of a rolling window, not drift. `CreateDelta`
  reports `Additive` or `None`, never `Rebuild`, for a boundary-set difference. A column or type change
  still rebuilds.
- **Retiring a period is `TRUNCATE` then `MERGE RANGE`, in that order.** `MERGE RANGE` on a partition
  that still holds rows does not reclaim anything — it *moves* those rows into the neighbouring
  partition, which is the opposite of the point. Truncating the partition first (`TRUNCATE TABLE ...
  WITH (PARTITIONS (n))`, SQL Server 2016+) deallocates its pages in O(1), and the merge that follows is
  then metadata-only against an empty partition. If the truncate fails, the boundary is deliberately
  left in place.

Retention only retires boundaries that land exactly on a period start for the policy, so a hand-added
boundary is left strictly alone. It also reclaims the partition below the oldest boundary once that
boundary ages out — that partition holds late-arriving rows dated before the window ever started, and it
is entirely below the retention floor.

::: warning
Retiring a period removes its rows. That is the point — it is what makes reclaim O(1) — but choose
`periodsBehind` to match the retention policy you actually want.
:::

## Managed Tenant Partitioning

For multi-tenant tables where each tenant gets its own physical partition, use
`PartitionByManagedTenants(...)` with a shared `ManagedTenantPartitions` manager. Tenants are allocated
integer ordinals at sign-up, the table is partitioned `RANGE RIGHT` on the ordinal column, and the
partition function is split for each new tenant at runtime. See the manager's API for
`AddPartitionToAllTables` / `DropPartitionFromAllTables`.

```cs
var manager = new ManagedTenantPartitions(
    "tenant-partitions",
    new DbObjectName("dbo", "tenant_partitions"));
table.PartitionByManagedTenants(manager);
```

Managed strategies own their boundaries at runtime, so — unlike `RangePartitioning` — they are not
diffed or migrated by `FindDeltaAsync`. When a table joins an *existing* managed set (tenants already
registered), call `MigrateAllTablesAsync(logger, database, token)` to back-fill every managed table
with the full set of registered tenant ordinals.

### Dropping tenants: data semantics

SQL Server's `MERGE RANGE` only removes the boundary point — the tenant's rows are merged into the
neighboring partition, not deleted. `DropPartitionFromAllTables` therefore takes a `TenantDropBehavior`:

- `TenantDropBehavior.RetainData` (default) — merge only; the caller owns any data purge.
- `TenantDropBehavior.DeleteData` — delete the tenant's rows from every managed table first, then merge.
  This is the hard-delete parity with the PostgreSQL managed drop (`DETACH PARTITION` + `DROP TABLE`).

With `DeleteData`, tables with a foreign key to another managed table are purged before the table they
reference, so the referenced rows are no longer in use when they are deleted. A self-referencing foreign
key doesn't matter. Tables whose foreign keys form a cycle can't be ordered: a `DELETE` that fails is
logged, and that table's boundary is left in place rather than merging the rows it still holds.

### Tenant bucketing (many tenants per partition)

By default every tenant gets its own ordinal, guarded by a unique index on the registry table. For
applications with many small tenants — or to stay clear of SQL Server's 15,000-partition-per-table
ceiling — set `AllowOrdinalSharing = true` (which removes the unique index) and assign ordinals
explicitly so several tenants share one partition:

```cs
manager.AllowOrdinalSharing = true;

var result = await manager.AddPartitionsToAllTables(logger, database,
    new Dictionary<string, int> { ["acme"] = 1, ["globex"] = 1, ["initech"] = 2 },
    token);
```

A shared ordinal is only merged (and only purged under `DeleteData`) once its *last* tenant is
dropped; while other tenants still map to it, the partition and all of its rows are left alone.

### Batch registration and status reporting

The batch `AddPartitionsToAllTables(...)` overloads return a `TenantPartitionAddResult` carrying both
the assigned `tenant_id -> ordinal` map and a per-table `TablePartitionStatus[]` (parity with the
PostgreSQL `ManagedListPartitions` batch add), so callers can surface partial failures — one table
failing to split no longer prevents the remaining tables from being split.

### A time limit for the SPLIT

A `SPLIT` waits for a schema modification lock on the table, and while it waits every later reader and
writer of that table queues behind it. To give up on the `SPLIT` sooner than the connection's command
timeout, set `SplitCommandTimeout`:

```cs
manager.SplitCommandTimeout = TimeSpan.FromSeconds(5);
```

It applies to each `ALTER PARTITION FUNCTION ... SPLIT RANGE` and the `NEXT USED` before it, and to
nothing else: loading the registry and writing the tenant into it keep the connection's command timeout.
A `SPLIT` that gives up is reported as `Failed` for that table like any other failed split, the tenant
keeps its ordinal, and `MigrateAllTablesAsync` (or the next registration) fills the boundary in later.
The default, `null`, leaves the `SPLIT` on the connection's command timeout.

To set it for one call instead, pass it to `AddPartitionsToAllTables`:

```cs
var result = await manager.AddPartitionsToAllTables(logger, database, ["acme"],
    TimeSpan.FromSeconds(5), token);
```

## Partition Function and Scheme Names

A partition function and a partition scheme belong to the *database*, not to a schema. So Weasel names
them after the schema as well as the table and column, `pf_{schema}_{table}_{column}` and
`ps_{schema}_{table}_{column}`, and the same table in two schemas of one database gets objects of its
own. That matters as soon as the two want different boundaries: two stores in different schemas, each
with its own tenants, or the same document table with different date ranges per schema
([weasel#920](https://github.com/JasperFx/weasel/issues/920)).

<!-- snippet: sample_ss_partition_object_names -->
<a id='snippet-sample_ss_partition_object_names'></a>
```cs
var table = new Table("tenant_a.orders");

// What a table partitioned from scratch gets: pf_tenant_a_orders_tenant_ordinal
var function = SqlServerPartitionObjects.FunctionNameFor(table, "tenant_ordinal");
var scheme = SqlServerPartitionObjects.SchemeNameFor(table, "tenant_ordinal");

// What 9.x and the 10.0 release candidates named it: pf_orders_tenant_ordinal
var legacy = SqlServerPartitionObjects.LegacyFunctionNameFor(table, "tenant_ordinal");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L141-L150' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_partition_object_names' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A name longer than SQL Server's 128 characters is shortened, and ends in a hash of the full name so two
long names can't collide.

::: warning Upgrading from 9.x or the 10.0 release candidates
Weasel 9.x and the 10.0 release candidates named these objects `pf_{table}_{column}` and
`ps_{table}_{column}`, with no schema. SQL Server can't rename a partition function, and moving a table
onto another scheme rewrites every row, so Weasel leaves a table that's already partitioned on the objects
it's on, whatever they're called:

- the delta compares boundaries, never names, so your existing tables stay clean and nothing is rebuilt
  or dropped;
- every `SPLIT RANGE`, `MERGE RANGE` and `NEXT USED` that migration, `ManagedRangePartitions` or
  `ManagedTenantPartitions` issues goes to the function and scheme the table is actually on.

Only a table that's partitioned from scratch gets the new names: a new table, a table moved onto its
partition scheme, or a table rebuilt for some other reason. You don't need to do anything, unless two of
your schemas already share a function.
:::

### Finding a function shared across schemas

If you ran more than one schema against one database before 10.0, the same table in each of them is on
one shared function, and every boundary one of them gets, they all get. Ask the database which functions
are shared:

<!-- snippet: sample_ss_find_shared_partition_functions -->
<a id='snippet-sample_ss_find_shared_partition_functions'></a>
```cs
// Partition functions that tables in more than one schema are partitioned on. Only a database
// partitioned by a Weasel older than 10.0 can have these by Weasel's doing (weasel#920)
var shared = await SqlServerPartitionObjects.FindSharedAcrossSchemasAsync(conn);

foreach (var function in shared)
{
    Console.WriteLine(
        $"{function.FunctionName} is shared by {string.Join(", ", function.Tables.Select(x => x.QualifiedName))}");
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L126-L136' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_find_shared_partition_functions' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`ManagedTenantPartitions` and `ManagedRangePartitions` also log a warning, once per function, the first
time they split or merge a function that a table in another schema is on.

### Moving a table off a shared function

Weasel won't do this for you, because it rewrites the whole table. Do it deliberately, in a maintenance
window, for every schema but one. For a table `tenant_a.orders` partitioned on `tenant_ordinal`:

```sql
-- 1. The function and scheme Weasel would create for this table today, with the boundaries this
--    table wants: its declared boundaries for RangePartitioning, or 0 and the ordinals in this
--    store's registry for ManagedTenantPartitions
CREATE PARTITION FUNCTION [pf_tenant_a_orders_tenant_ordinal] (int)
    AS RANGE RIGHT FOR VALUES (0, 1, 2);
CREATE PARTITION SCHEME [ps_tenant_a_orders_tenant_ordinal]
    AS PARTITION [pf_tenant_a_orders_tenant_ordinal] ALL TO ([PRIMARY]);

-- 2. Rebuild the clustered primary key on the new scheme. This moves every row. Drop any foreign
--    key that references this key first, and add it back afterwards
ALTER TABLE [tenant_a].[orders] DROP CONSTRAINT [pkey_orders_tenant_ordinal_id];
ALTER TABLE [tenant_a].[orders] ADD CONSTRAINT [pkey_orders_tenant_ordinal_id]
    PRIMARY KEY CLUSTERED ([tenant_ordinal], [id])
    ON [ps_tenant_a_orders_tenant_ordinal]([tenant_ordinal]);

-- 3. Every other index on the table is aligned with the old scheme; rebuild each one on the new one
CREATE INDEX [ix_orders_placed_at] ON [tenant_a].[orders] ([placed_at])
    WITH (DROP_EXISTING = ON) ON [ps_tenant_a_orders_tenant_ordinal]([tenant_ordinal]);
```

Once no table is on the old function and scheme any more, drop them:

```sql
DROP PARTITION SCHEME [ps_orders_tenant_ordinal];
DROP PARTITION FUNCTION [pf_orders_tenant_ordinal];
```

::: tip
If the table's data is disposable, say in development or test, it's simpler to drop the table and let
Weasel create it again: it gets the new names, and the old function and scheme can be dropped once
nothing is on them.
:::
