using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

[Collection("partitions")]
public class integer_list_partition_round_trip : IntegrationContext
{
    private readonly Table theTable;

    public integer_list_partition_round_trip() : base("partitions")
    {
        theTable = new Table("partitions.people");
        theTable.AddColumn<int>("id").AsPrimaryKey();
        theTable.AddColumn<string>("first_name");
        theTable.AddColumn<int>("age")
            .PartitionByListValues()
            .AddPartition("twenties", 20, 21, 22)
            .AddPartition("thirties", 30, 31, 32);
    }

    private async Task tryToCreateTable()
    {
        await theConnection.OpenAsync();
        await theConnection.ResetSchemaAsync("partitions");
        await theTable.CreateAsync(theConnection);
    }

    [Fact]
    public async Task no_delta_on_reapply_for_integer_list_values()
    {
        await tryToCreateTable();

        // Before weasel#320: declared integer values (20) never string-matched PostgreSQL's quoted
        // read-back ('20'), so every migration reported a spurious, destructive partition rebuild.
        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task fetch_existing_matches_declared_integer_list_partitions()
    {
        await tryToCreateTable();

        var existing = await theTable.FetchExistingAsync(theConnection);

        var partitioning = existing.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.Columns.Single().ShouldBe("age");

        // ShouldContain uses Equals, which normalizes the quoted read-back back to the declared form
        partitioning.Partitions.ShouldContain(new ListPartition("twenties", "20", "21", "22"));
        partitioning.Partitions.ShouldContain(new ListPartition("thirties", "30", "31", "32"));
    }
}
