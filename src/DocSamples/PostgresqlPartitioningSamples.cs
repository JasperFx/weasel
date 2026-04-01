using Microsoft.Extensions.Logging;
using Weasel.Core;
using Weasel.Postgresql;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;

namespace DocSamples;

public class PostgresqlPartitioningSamples
{
    public void hash_partitioning()
    {
        #region sample_pg_hash_partitioning
        var table = new Table("events");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("category").NotNull();
        table.AddColumn("data", "jsonb");

        table.PartitionByHash(new HashPartitioning
        {
            Columns = new[] { "id" },
            Suffixes = new[] { "p0", "p1", "p2", "p3" }
        });
        #endregion
    }

    public void range_partitioning()
    {
        #region sample_pg_range_partitioning
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
        #endregion
    }

    public void list_partitioning()
    {
        #region sample_pg_list_partitioning
        var table = new Table("orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("region").NotNull();
        table.AddColumn<decimal>("total");

        var partitioning = table.PartitionByList("region");
        partitioning.AddPartition("north", "US-NORTH", "CA-NORTH");
        partitioning.AddPartition("south", "US-SOUTH", "MX");
        #endregion
    }

    public void managed_list_partitions()
    {
        #region sample_pg_managed_list_partitions
        var table = new Table("tenanted_data");

        var manager = new ManagedListPartitions(
            "tenant_partitions",
            new DbObjectName("public", "mt_tenant_partitions"));

        var partitioning = table.PartitionByList("tenant_id");
        partitioning.UsePartitionManager(manager);
        #endregion
    }

    public async Task add_partition_at_runtime()
    {
        #region sample_pg_add_partition_at_runtime
        PostgresqlDatabase database = null!; // your database instance
        ManagedListPartitions manager = null!; // your partition manager
        var ct = CancellationToken.None;

        await manager.AddPartitionToAllTables(database, "tenant_a", "tenant_a", ct);
        #endregion
    }

    public async Task add_multiple_partitions()
    {
        #region sample_pg_add_multiple_partitions
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
        #endregion
    }

    public async Task drop_partition()
    {
        #region sample_pg_drop_partition
        PostgresqlDatabase database = null!; // your database instance
        ManagedListPartitions manager = null!; // your partition manager
        ILogger logger = null!; // your logger
        var ct = CancellationToken.None;

        await manager.DropPartitionFromAllTablesForValue(database, logger, "tenant_a", ct);
        #endregion
    }
}
