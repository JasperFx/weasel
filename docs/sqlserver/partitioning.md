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
CREATE PARTITION FUNCTION [pf_metrics_sample_bucket_end] (datetime)
    AS RANGE RIGHT FOR VALUES ('2026-01-01 00:00:00', '2026-02-01 00:00:00', '2026-03-01 00:00:00');
CREATE PARTITION SCHEME [ps_metrics_sample_bucket_end]
    AS PARTITION [pf_metrics_sample_bucket_end] ALL TO ([PRIMARY]);
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

<!-- snippet: sample_ss_rolling_window_partitioning -->
<a id='snippet-sample_ss_rolling_window_partitioning'></a>
```cs
// One partition per month, three months provisioned ahead of now,
// six completed months retained before a period is aged out
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L411-L428' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_rolling_window_partitioning' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

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

<!-- snippet: sample_ss_roll_range_window_forward -->
<a id='snippet-sample_ss_roll_range_window_forward'></a>
```cs
// Split in any missing boundaries at the leading edge, then truncate and
// merge away everything below the retention floor. Idempotent, so this is
// safe on every startup and on a timer
await manager.ApplyAsync(database, logger, token);

// ...or run just one half
await manager.RollForwardAsync(database, logger, token);
await manager.DropAgedPartitionsAsync(database, logger, token);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L438-L447' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_roll_range_window_forward' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

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
