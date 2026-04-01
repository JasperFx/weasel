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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L13-L24' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_hash_partitioning' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L29-L42' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_range_partitioning' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L47-L56' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_list_partitioning' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L61-L70' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_managed_list_partitions' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L75-L81' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_add_partition_at_runtime' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L86-L98' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_add_multiple_partitions' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlPartitioningSamples.cs#L103-L110' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_drop_partition' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection

Weasel detects partition changes during migration. The `PartitionDelta` enum indicates:

- **None** -- partitions match the expected configuration
- **Additive** -- new partitions can be added without rebuilding
- **Rebuild** -- partition strategy changed and requires table recreation

Set `table.IgnorePartitionsInMigration = true` if an external tool like `pg_partman` manages your partitions.
