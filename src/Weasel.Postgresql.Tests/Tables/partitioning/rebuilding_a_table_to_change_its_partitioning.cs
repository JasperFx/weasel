using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

/// <summary>
///     weasel#871: a change of partitioning rebuilds the table (copy aside, drop, create, copy back). The
///     primary key and index changes used to be written against the old table first, and PostgreSQL
///     refuses a primary key without the partition column on a table that is still partitioned (0A000).
/// </summary>
[Collection("partitions")]
public class rebuilding_a_table_to_change_its_partitioning: IntegrationContext
{
    public rebuilding_a_table_to_change_its_partitioning(): base("partitions")
    {
    }

    private static Table partitionedEvents()
    {
        var table = new Table("partitions.events");
        table.AddColumn<long>("seq_id").AsPrimaryKey();
        table.AddColumn<bool>("is_archived").AsPrimaryKey().DefaultValueByExpression("false")
            .PartitionByListValues()
            .AddPartition("archived", true);
        table.AddColumn<string>("data");
        table.Indexes.Add(new IndexDefinition("idx_events_data") { Columns = ["data", "is_archived"] });

        return table;
    }

    private static Table unpartitionedEvents()
    {
        var table = new Table("partitions.events");
        table.AddColumn<long>("seq_id").AsPrimaryKey();
        table.AddColumn<bool>("is_archived").DefaultValueByExpression("false");
        table.AddColumn<string>("data");
        table.Indexes.Add(new IndexDefinition("idx_events_data") { Columns = ["data"] });

        return table;
    }

    private async Task insertRowsAsync()
    {
        await theConnection.CreateCommand(
                "insert into partitions.events (seq_id, is_archived, data) values (1, false, 'one'), (2, true, 'two')")
            .ExecuteNonQueryAsync();
    }

    private async Task<long> countRowsAsync()
    {
        var count = await theConnection.CreateCommand("select count(*) from partitions.events").ExecuteScalarAsync();
        return Convert.ToInt64(count);
    }

    [Fact]
    public async Task remove_partitioning_when_the_primary_key_loses_the_partition_column()
    {
        await ResetSchema();

        await CreateSchemaObjectInDatabase(partitionedEvents());
        await insertRowsAsync();

        var expected = unpartitionedEvents();
        var delta = await expected.FindDeltaAsync(theConnection);
        delta.PartitionDelta.ShouldBe(PartitionDelta.Rebuild);

        await expected.MigrateAsync(theConnection);

        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await countRowsAsync()).ShouldBe(2);
    }

    [Fact]
    public async Task add_partitioning_when_the_primary_key_gains_the_partition_column()
    {
        await ResetSchema();

        await CreateSchemaObjectInDatabase(unpartitionedEvents());
        await insertRowsAsync();

        var expected = partitionedEvents();
        var delta = await expected.FindDeltaAsync(theConnection);
        delta.PartitionDelta.ShouldBe(PartitionDelta.Rebuild);

        await expected.MigrateAsync(theConnection);

        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await countRowsAsync()).ShouldBe(2);
    }

    [Fact]
    public void the_rebuild_makes_no_primary_key_or_index_changes_against_the_old_table()
    {
        var delta = new TableDelta(unpartitionedEvents(), partitionedEvents());
        delta.PartitionDelta.ShouldBe(PartitionDelta.Rebuild);

        var writer = new StringWriter();
        delta.WriteUpdate(new PostgresqlMigrator(), writer);
        var ddl = writer.ToString();

        var copyAside = ddl.IndexOf("create table partitions.events_temp", StringComparison.Ordinal);
        copyAside.ShouldBeGreaterThanOrEqualTo(0);

        var beforeTheRebuild = ddl.Substring(0, copyAside);
        beforeTheRebuild.ShouldNotContain("constraint");
        beforeTheRebuild.ShouldNotContain("index", Case.Insensitive);
    }
}
