using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables.partitioning;

[Collection("integration")]
public class range_partitioning_round_trip : IntegrationContext
{
    public range_partitioning_round_trip() : base("partitioning")
    {
    }

    private Table MetricsTable(params DateTime[] boundaries)
    {
        var table = new Table("partitioning.metrics_sample");
        table.AddColumn<int>("id");
        table.AddColumn("bucket_end", "datetime").NotNull();
        table.AddColumn<string>("data");

        // The partition column must participate in the primary key on SQL Server.
        table.ModifyColumn("id").AsPrimaryKey();
        table.ModifyColumn("bucket_end").AsPrimaryKey();

        var partitioning = table.PartitionByRange("bucket_end", "datetime");
        foreach (var boundary in boundaries)
        {
            partitioning.AddBoundary(boundary);
        }

        return table;
    }

    private async Task<int> PartitionCountAsync(string tableName)
    {
        var count = await theConnection.CreateCommand($"""
            SELECT COUNT(*)
            FROM sys.partitions p
            JOIN sys.objects o ON p.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = 'partitioning' AND o.name = '{tableName}' AND p.index_id IN (0, 1)
            """).ExecuteScalarAsync();

        return (int)count!;
    }

    [Fact]
    public async Task fetch_existing_reads_partition_metadata()
    {
        await ResetSchema();

        var table = MetricsTable(new DateTime(2026, 1, 1), new DateTime(2026, 2, 1));
        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);

        existing.ShouldNotBeNull();
        existing!.PartitionInfo.ShouldNotBeNull();
        existing.PartitionInfo!.Column.ShouldBe("bucket_end");
        existing.PartitionInfo.SqlDataType.ShouldBe("datetime");
        existing.PartitionInfo.IsRangeRight.ShouldBeTrue();
        existing.PartitionInfo.BoundaryValues.ShouldBe(
            ["'2026-01-01 00:00:00'", "'2026-02-01 00:00:00'"]);
    }

    [Fact]
    public async Task no_delta_for_an_unchanged_range_partitioned_table()
    {
        await ResetSchema();

        var table = MetricsTable(new DateTime(2026, 1, 1), new DateTime(2026, 2, 1));
        await CreateSchemaObjectInDatabase(table);

        var delta = await table.FindDeltaAsync(theConnection);

        delta.PartitioningDifference.ShouldBe(SchemaPatchDifference.None);
        delta.HasChanges().ShouldBeFalse();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task additive_boundary_is_detected_and_applied_with_split_range()
    {
        await ResetSchema();

        var initial = MetricsTable(new DateTime(2026, 1, 1), new DateTime(2026, 2, 1));
        await CreateSchemaObjectInDatabase(initial);
        (await PartitionCountAsync("metrics_sample")).ShouldBe(3); // 2 boundaries -> 3 partitions

        // Roll a new month forward.
        var rolledForward = MetricsTable(
            new DateTime(2026, 1, 1), new DateTime(2026, 2, 1), new DateTime(2026, 3, 1));

        var delta = await rolledForward.FindDeltaAsync(theConnection);
        delta.PartitioningDifference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();

        var writer = new StringWriter();
        delta.WriteUpdate(new SqlServerMigrator(), writer);
        writer.ToString().ShouldContain("SPLIT RANGE ('2026-03-01 00:00:00')");

        await rolledForward.ApplyChangesAsync(theConnection);

        // The new boundary added a partition...
        (await PartitionCountAsync("metrics_sample")).ShouldBe(4);

        // ...and the table now round-trips clean.
        var after = await rolledForward.FindDeltaAsync(theConnection);
        after.HasChanges().ShouldBeFalse();
    }

    [Fact]
    public async Task removing_a_boundary_is_a_rebuild()
    {
        await ResetSchema();

        var initial = MetricsTable(new DateTime(2026, 1, 1), new DateTime(2026, 2, 1));
        await CreateSchemaObjectInDatabase(initial);

        var fewerBoundaries = MetricsTable(new DateTime(2026, 2, 1));

        var delta = await fewerBoundaries.FindDeltaAsync(theConnection);

        delta.PartitioningDifference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
    }

    [Fact]
    public async Task bit_archived_partitioning_round_trips_with_no_delta()
    {
        // Regression guard for the consumer pattern (Polecat/Marten archived-events tables): a bit
        // column partitioned RANGE RIGHT at boundary 1 must read back identically and report no delta.
        await ResetSchema();

        var table = new Table("partitioning.archived_events");
        table.AddColumn<int>("id");
        table.AddColumn<bool>("is_archived").NotNull();
        table.AddColumn<string>("data");
        table.ModifyColumn("id").AsPrimaryKey();
        table.ModifyColumn("is_archived").AsPrimaryKey();
        table.PartitionByRange("is_archived", "bit").AddBoundary(1);

        await CreateSchemaObjectInDatabase(table);

        var delta = await table.FindDeltaAsync(theConnection);

        delta.PartitioningDifference.ShouldBe(SchemaPatchDifference.None);
        delta.HasChanges().ShouldBeFalse();
    }
}
