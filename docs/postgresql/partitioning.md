# Table Partitioning

Weasel supports PostgreSQL's native table partitioning via the `IPartitionStrategy` interface, with built-in implementations for hash, range, and list partitioning.

## Hash Partitioning

Distributes rows across a fixed number of partitions using a hash of the partition key columns.

<!-- snippet: sample_pg_hash_partitioning -->
<a id='snippet-sample_pg_hash_partitioning'></a>
```cs
var table = new Table("events");
table.AddColumn<Guid>("id").AsPrimaryKey();
table.AddColumn<string>("category").NotNull();
table.AddColumn("data", "jsonb");

table.PartitionByHash(new HashPartitioning
{
    Columns = new[] { "id" },
    Suffixes = new[] { "p0", "p1", "p2", "p3" }
});
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L14-L25' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_hash_partitioning' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The `Suffixes` property automatically calculates the modulus and remainder for each partition. The resulting partition tables are named `{table}_{suffix}`.

## Range Partitioning

Splits rows into partitions based on value ranges. A default partition is created automatically to catch values outside defined ranges.

<!-- snippet: sample_pg_range_partitioning -->
<a id='snippet-sample_pg_range_partitioning'></a>
```cs
var table = new Table("measurements");
table.AddColumn<int>("id").AsPrimaryKey();
table.AddColumn<DateTimeOffset>("recorded_at").NotNull();
table.AddColumn<double>("value");

var partitioning = table.PartitionByRange("recorded_at");
partitioning.AddRange("q1_2024",
    DateTimeOffset.Parse("2024-01-01"),
    DateTimeOffset.Parse("2024-04-01"));
partitioning.AddRange("q2_2024",
    DateTimeOffset.Parse("2024-04-01"),
    DateTimeOffset.Parse("2024-07-01"));
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L30-L43' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_range_partitioning' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## List Partitioning

Assigns rows to partitions based on discrete column values.

<!-- snippet: sample_pg_list_partitioning -->
<a id='snippet-sample_pg_list_partitioning'></a>
```cs
var table = new Table("orders");
table.AddColumn<int>("id").AsPrimaryKey();
table.AddColumn<string>("region").NotNull();
table.AddColumn<decimal>("total");

var partitioning = table.PartitionByList("region");
partitioning.AddPartition("north", "US-NORTH", "CA-NORTH");
partitioning.AddPartition("south", "US-SOUTH", "MX");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L48-L57' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_list_partitioning' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A default partition is enabled by default. Disable it with `partitioning.DisableDefaultPartition()`.

## ManagedListPartitions

For dynamic partition management (e.g., multi-tenant systems), use `ManagedListPartitions`. This stores partition assignments in a dedicated database table and can add or remove partitions at runtime.

<!-- snippet: sample_pg_managed_list_partitions -->
<a id='snippet-sample_pg_managed_list_partitions'></a>
```cs
var table = new Table("tenanted_data");

var manager = new ManagedListPartitions(
    "tenant_partitions",
    new DbObjectName("public", "mt_tenant_partitions"));

var partitioning = table.PartitionByList("tenant_id");
partitioning.UsePartitionManager(manager);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L62-L71' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_managed_list_partitions' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

At runtime, add partitions across all managed tables:

<!-- snippet: sample_pg_add_partition_at_runtime -->
<a id='snippet-sample_pg_add_partition_at_runtime'></a>
```cs
PostgresqlDatabase database = null!; // your database instance
ManagedListPartitions manager = null!; // your partition manager
var ct = CancellationToken.None;

await manager.AddPartitionToAllTables(database, "tenant_a", "tenant_a", ct);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L76-L82' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_add_partition_at_runtime' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Or add multiple partitions at once:

<!-- snippet: sample_pg_add_multiple_partitions -->
<a id='snippet-sample_pg_add_multiple_partitions'></a>
```cs
PostgresqlDatabase database = null!; // your database instance
ManagedListPartitions manager = null!; // your partition manager
ILogger logger = null!; // your logger
var ct = CancellationToken.None;

var values = new Dictionary<string, string>
{
    { "tenant_b", "tenant_b" },
    { "tenant_c", "tenant_c" }
};
await manager.AddPartitionToAllTables(logger, database, values, ct);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L87-L99' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_add_multiple_partitions' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Remove partitions when a tenant is deprovisioned:

<!-- snippet: sample_pg_drop_partition -->
<a id='snippet-sample_pg_drop_partition'></a>
```cs
PostgresqlDatabase database = null!; // your database instance
ManagedListPartitions manager = null!; // your partition manager
ILogger logger = null!; // your logger
var ct = CancellationToken.None;

await manager.DropPartitionFromAllTablesForValue(database, logger, "tenant_a", ct);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L104-L111' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_drop_partition' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## ManagedRangePartitions

Time-series tables -- telemetry, audit trails, events, logs -- want a partition set that *moves*. The
declared window rolls forward every period, last period's partitions are still on disk, and the point of
partitioning them at all is that retiring a period should be a `DROP TABLE` instead of a mass `DELETE`
and the vacuum storm that follows.

`ManagedRangePartitions` owns that window. Rather than declaring a static list of ranges, you declare
intent -- a period, how many periods to provision ahead, and how many to retain behind -- and Weasel
writes every DDL statement:

<!-- snippet: sample_pg_managed_range_partitions -->
<a id='snippet-sample_pg_managed_range_partitions'></a>
```cs
var table = new Table("metrics");
table.AddColumn<Guid>("id").AsPrimaryKey();

// PostgreSQL requires the partition key to be part of every unique
// constraint on a partitioned table
table.AddColumn<DateTimeOffset>("occurred_at").AsPrimaryKey().NotNull();
table.AddColumn<double>("value");

// One partition per month, three months provisioned ahead of now,
// six completed months retained before a partition is aged out
var manager = new ManagedRangePartitions(
    RollingWindowPolicy.Monthly(periodsAhead: 3, periodsBehind: 6));

table.PartitionByRange("occurred_at").UsePartitionManager(manager);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L116-L131' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_managed_range_partitions' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`RollingWindowPolicy` supports `Hourly`, `Daily`, `Weekly`, `Monthly`, and `Yearly` windows. Weekly
partitions begin on Monday by default; set `FirstDayOfWeek` to change that at create time. All boundary
arithmetic is done in UTC.

Partition tables are named `{table}_{suffix}`, where the suffix encodes the period start: `y2026`,
`m202607`, `w20260727`, `d20260730`, `h2026073014`. A `DEFAULT` overflow partition is always created, so
a row outside the provisioned window is never rejected.

Roll the window forward and retire aged partitions at runtime:

<!-- snippet: sample_pg_roll_range_window_forward -->
<a id='snippet-sample_pg_roll_range_window_forward'></a>
```cs
PostgresqlDatabase database = null!; // your database instance
ManagedRangePartitions manager = null!; // your partition manager
ILogger logger = null!; // your logger
var ct = CancellationToken.None;

// Create any missing periods at the leading edge and drop everything
// older than the retention floor. Idempotent, so this is safe on every
// startup and from every node
await manager.ApplyAsync(database, logger, ct);

// ...or run just one half of it
await manager.RollForwardAsync(database, logger, ct);
await manager.DropAgedPartitionsAsync(database, logger, ct);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L136-L150' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_roll_range_window_forward' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Both halves are idempotent and safe to run concurrently from several nodes, so calling `ApplyAsync` on
startup and on a timer is the intended usage.

Two properties make this different from a static range declaration:

- **Rolling forward is always `Additive`, never `Rebuild`.** The declarative range strategy treats
  "the actual database has partitions the declaration no longer names" as drift and rebuilds the table.
  For a rolling window that is the normal steady state, and a rebuild of a multi-gigabyte table would be
  triggered by nothing more than the calendar. With a partition manager attached, migration only ever
  adds partitions.
- **Aged partitions are a policy outcome, not drift.** Retention drops them, and only ever drops
  partitions whose suffix this policy itself produced. A hand-created partition, or one left over from a
  different period size, is left strictly alone.

Because of that, a rolling time-partitioned table needs neither `IgnorePartitionsInMigration` nor an
"externally managed" escape hatch -- so it keeps Weasel's ordering and dependency management instead of
hand-writing `CREATE TABLE ... PARTITION OF` and `DROP TABLE` in application code.

::: warning
Dropping an aged partition removes its rows. That is the point -- it is what makes reclaim O(1) -- but
choose `periodsBehind` with the retention policy you actually want.
:::

## Thread Safety

`ManagedListPartitions` uses double-checked locking with a semaphore to safely initialize the partition map from the database. It is safe to call `InitializeAsync`, `AddPartitionToAllTables`, and `DropPartitionFromAllTablesForValue` concurrently from multiple threads.

## Delta Detection

Weasel detects partition changes during migration. The `PartitionDelta` enum indicates:

- **None** -- partitions match the expected configuration
- **Additive** -- new partitions can be added without rebuilding
- **Rebuild** -- partition strategy changed and requires table recreation

Set `table.IgnorePartitionsInMigration = true` if an external tool like `pg_partman` manages your partitions.
A table using `ManagedListPartitions` or `ManagedRangePartitions` does not need it -- those strategies are
already exempt from the rebuild path.
