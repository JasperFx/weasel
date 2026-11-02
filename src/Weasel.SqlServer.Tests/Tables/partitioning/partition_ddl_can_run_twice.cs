using System;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Partitioning;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables.partitioning;

/// <summary>
///     weasel#765. The generated creation script for a partitioned table could not be run twice: the
///     partition DDL dropped the scheme with no check for the tables already using it, so the second
///     execution failed on any partitioned shape.
/// </summary>
/// <remarks>
///     Found through Polecat's <c>generated_schema_script_executes</c>, which runs each generated script
///     twice to catch drift. Partitioned shapes had to run it once and prove convergence with a second
///     apply instead, so that check never reached them (polecat#842).
/// </remarks>
[Collection("integration")]
public class partition_ddl_can_run_twice : IntegrationContext
{
    public partition_ddl_can_run_twice() : base("gh765")
    {
    }

    private static Table RangeTable()
    {
        var table = new Table("gh765.metrics");
        table.AddColumn<int>("id");
        table.AddColumn("bucket_end", "datetime").NotNull();
        table.AddColumn<string>("data");

        // The partition column must participate in the primary key on SQL Server.
        table.ModifyColumn("id").AsPrimaryKey();
        table.ModifyColumn("bucket_end").AsPrimaryKey();

        var partitioning = table.PartitionByRange("bucket_end", "datetime");
        partitioning.AddBoundary(new DateTime(2026, 1, 1));
        partitioning.AddBoundary(new DateTime(2026, 2, 1));

        return table;
    }

    [Fact]
    public async Task the_range_partitioned_creation_script_is_idempotent()
    {
        await ResetSchema();

        var table = RangeTable();

        await CreateSchemaObjectInDatabase(table);

        // The second execution is the one that threw: DROP PARTITION SCHEME with the table still
        // bound to it.
        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);
        existing.ShouldNotBeNull();
        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_rolling_window_creation_script_is_idempotent()
    {
        await ResetSchema();

        var table = new Table("gh765.rolling");
        table.AddColumn<int>("id");
        table.AddColumn("event_time", "datetime").NotNull();
        table.ModifyColumn("id").AsPrimaryKey();
        table.ModifyColumn("event_time").AsPrimaryKey();

        table.PartitionByRollingWindow(
            new ManagedRangePartitions(RollingWindowPolicy.Monthly(1, 2), "event_time", "datetime"));

        await CreateSchemaObjectInDatabase(table);
        await CreateSchemaObjectInDatabase(table);

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Running the script a second time must not quietly abandon the partitioning either: skipping
    ///     the drop is only correct if the scheme that survives is the one the table is meant to be on.
    /// </summary>
    [Fact]
    public async Task the_table_is_still_partitioned_after_a_second_run()
    {
        await ResetSchema();

        var table = RangeTable();
        await CreateSchemaObjectInDatabase(table);
        await CreateSchemaObjectInDatabase(table);

        var count = await theConnection.CreateCommand("""
            SELECT COUNT(DISTINCT p.partition_number)
            FROM sys.partitions p
            WHERE p.object_id = OBJECT_ID('gh765.metrics')
            """).ExecuteScalarAsync();

        // Two boundaries, RANGE RIGHT -> three partitions.
        Convert.ToInt32(count).ShouldBe(3);
    }
}
