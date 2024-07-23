using Shouldly;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

[Collection("partitions")]
public class hash_partitions: IntegrationContext
{
    private readonly Table theTable;

    public hash_partitions() : base("partitions")
    {
        theTable = new Table("partitions.people");
        theTable.AddColumn<int>("id").AsPrimaryKey();
        theTable.AddColumn<string>("first_name");
        theTable.AddColumn<string>("last_name");
        theTable.AddColumn<string>("role")
            .PartitionByHash("one", "two", "three");
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
    public async Task write_sql_for_partition_by_hash()
    {
        var sql = theTable.ToCreateSql(new PostgresqlMigrator());

        sql.ShouldContain("PARTITION BY HASH (role)");

        sql.ShouldContain("create table partitions.people_one partition of partitions.people for values with (modulus 3, remainder 0);");
        sql.ShouldContain("create table partitions.people_two partition of partitions.people for values with (modulus 3, remainder 1);");
        sql.ShouldContain("create table partitions.people_three partition of partitions.people for values with (modulus 3, remainder 2);");

        await tryToCreateTable();
    }

    [Fact]
    public async Task fetch_the_existing_tables()
    {
        await tryToCreateTable();

        var existing = await tryToFetchExisting();

        var partitioning = existing.Partitioning.ShouldBeOfType<HashPartitioning>();

        partitioning.Columns.Single().ShouldBe("role");

        partitioning.Partitions.Count.ShouldBe(3);
        partitioning.Partitions.ShouldContain(new HashPartition("one", 3, 0));
        partitioning.Partitions.ShouldContain(new HashPartition("two", 3, 1));
        partitioning.Partitions.ShouldContain(new HashPartition("three", 3, 2));
    }

    [Fact]
    public void parse_expression()
    {
        var partition = HashPartition.Parse("suffix","FOR VALUES WITH (modulus 3, remainder 0)");
        partition.Modulus.ShouldBe(3);
        partition.Remainder.ShouldBe(0);
        partition.Suffix.ShouldBe("suffix");
    }

}
