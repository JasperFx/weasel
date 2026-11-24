using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Core.Partitioning;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;

namespace DocSamples;

// docs/sqlserver/partitioning.md samples
public class SqlServerPartitioningSamples
{
    public void range_partitioning()
    {
        #region sample_ss_range_partitioning
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
        #endregion
    }

    public async Task partitioning_delta(Table table, SqlConnection connection)
    {
        #region sample_ss_partitioning_delta
        var delta = await table.FindDeltaAsync(connection);

        // SchemaPatchDifference.None    -> the table matches the database
        // SchemaPatchDifference.Update  -> new boundaries can be rolled forward in place
        // SchemaPatchDifference.Invalid -> the partitioning must be rebuilt
        Console.WriteLine(delta.PartitioningDifference);
        #endregion
    }

    public async Task roll_forward_boundaries(Table table, SqlConnection connection)
    {
        #region sample_ss_roll_forward_boundaries
        // The database already has boundaries for Jan/Feb 2026. Declare March as well...
        var partitioning = table.PartitionByRange("bucket_end", "datetime");
        partitioning.AddBoundary(new DateTime(2026, 1, 1));
        partitioning.AddBoundary(new DateTime(2026, 2, 1));
        partitioning.AddBoundary(new DateTime(2026, 3, 1)); // new

        // ...and Weasel emits: ALTER PARTITION FUNCTION [...]() SPLIT RANGE ('2026-03-01 00:00:00');
        await table.ApplyChangesAsync(connection);
        #endregion
    }

    public async Task partition_an_existing_table(SqlConnection connection)
    {
        #region sample_ss_partition_an_existing_table
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
        #endregion
    }

    public void rolling_window()
    {
        #region sample_ss_rolling_window_partitioning
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
        #endregion
    }

    public async Task rolling_window_at_runtime(ManagedRangePartitions manager, IDatabase<SqlConnection> database,
        ILogger logger, CancellationToken token)
    {
        #region sample_ss_rolling_window_apply
        // Split in any missing boundaries at the leading edge, then truncate and merge
        // away everything below the retention floor. Idempotent, so this is safe on
        // every startup and on a timer.
        await manager.ApplyAsync(database, logger, token);

        // ...or run just one half
        await manager.RollForwardAsync(database, logger, token);
        await manager.DropAgedPartitionsAsync(database, logger, token);
        #endregion
    }

    public void managed_tenant_partitions(Table table)
    {
        #region sample_ss_managed_tenant_partitions
        var manager = new ManagedTenantPartitions(
            "tenant-partitions",
            new DbObjectName("dbo", "tenant_partitions"));

        table.PartitionByManagedTenants(manager);
        #endregion
    }

    public async Task tenant_bucketing(ManagedTenantPartitions manager, IDatabase<SqlConnection> database,
        ILogger logger, CancellationToken token)
    {
        #region sample_ss_tenant_bucketing
        manager.AllowOrdinalSharing = true;

        var result = await manager.AddPartitionsToAllTables(logger, database,
            new Dictionary<string, int> { ["acme"] = 1, ["globex"] = 1, ["initech"] = 2 },
            token);
        #endregion
    }

    public void split_command_timeout(ManagedTenantPartitions manager)
    {
        #region sample_ss_split_command_timeout
        manager.SplitCommandTimeout = TimeSpan.FromSeconds(5);
        #endregion
    }

    public async Task split_command_timeout_for_one_call(ManagedTenantPartitions manager,
        IDatabase<SqlConnection> database, ILogger logger, CancellationToken token)
    {
        #region sample_ss_split_command_timeout_for_one_call
        var result = await manager.AddPartitionsToAllTables(logger, database, ["acme"],
            TimeSpan.FromSeconds(5), token);
        #endregion
    }
}
