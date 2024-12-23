using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;


[Collection("partitions")]
public class range_partitions: IntegrationContext
{
    private readonly Table theTable;

    public range_partitions() : base("partitions")
    {
        theTable = new Table("partitions.people");
        theTable.AddColumn<int>("id").AsPrimaryKey();
        theTable.AddColumn<string>("first_name");
        theTable.AddColumn<string>("last_name");
        theTable.AddColumn<string>("role")
            .PartitionByRange()
            .AddRange("twenties", 20, 29)
            .AddRange("thirties", 30, 39);
    }

    [Fact]
    public void partition_table_names()
    {
        theTable.PartitionTableNames().ToArray()
            .ShouldBe(["people_twenties", "people_thirties", "people_default"]);

    }


    [Fact]
    public async Task write_sql_for_partition_by_list()
    {
        theTable.PartitionByList("role");

        var sql = theTable.ToCreateSql(new PostgresqlMigrator());

        sql.ShouldContain("PARTITION BY LIST (role)");

        await tryToCreateTable();
    }

    private async Task tryToCreateTable()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("partitions");

        await theTable.CreateAsync(theConnection);
    }

    private Task<Table> tryToFetchExisting()
    {
        return theTable.FetchExistingAsync(theConnection);
    }

    [Fact]
    public async Task create_table()
    {
        var sql = theTable.ToCreateSql(new PostgresqlMigrator());
        sql.ShouldContain("partitions.people_twenties");
        sql.ShouldContain("partitions.people_thirties");
        sql.ShouldContain("partitions.people_default");

        sql.ShouldContain("PARTITION BY RANGE (role)");

        await tryToCreateTable();
    }

    [Fact]
    public async Task fetch_existing_table()
    {
        await tryToCreateTable();
        var existing = await tryToFetchExisting();

        var partitioning = existing.Partitioning.ShouldBeOfType<RangePartitioning>();
        partitioning.Columns.Single().ShouldBe("role");

        partitioning.Ranges.Count.ShouldBe(2);

        partitioning.Ranges.ShouldContain(new RangePartition("twenties", "'20'", "'29'"));
        partitioning.Ranges.ShouldContain(new RangePartition("thirties", "'30'", "'39'"));
        partitioning.HasExistingDefault.ShouldBeTrue();
    }

    [Fact]
    public void parse_expression()
    {
        var range = RangePartition.Parse(new DbObjectName("partitions", "people"), "people_twenties",
            "FOR VALUES FROM ('20') TO ('29')");

        range.Suffix.ShouldBe("twenties");
        range.From.ShouldBe("'20'");
        range.To.ShouldBe("'29'");
    }
}
