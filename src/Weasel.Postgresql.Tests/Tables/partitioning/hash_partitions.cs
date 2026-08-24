using Shouldly;
using Weasel.Core;
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
    public async Task reattaching_a_partition_is_not_drift()
    {
        await tryToCreateTable();

        // DETACH/ATTACH deletes and re-inserts the pg_inherits row, which moves it to the end of
        // the heap -- the same reordering a parallel pg_restore or a recreated partition produces.
        await theConnection.CreateCommand(
                "alter table partitions.people detach partition partitions.people_two")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand(
                "alter table partitions.people attach partition partitions.people_two for values with (modulus 3, remainder 1)")
            .ExecuteNonQueryAsync();

        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void get_partition_table_names()
    {
        theTable.PartitionTableNames()
            .ShouldBe(["people_one", "people_two", "people_three"]);
    }

    [Fact]
    public async Task write_sql_for_partition_by_hash()
    {
        var sql = theTable.ToCreateSql(new PostgresqlMigrator());

        sql.ShouldContain("PARTITION BY HASH (role)");

        sql.ShouldContain("create table if not exists partitions.people_one partition of partitions.people for values with (modulus 3, remainder 0);");
        sql.ShouldContain("create table if not exists partitions.people_two partition of partitions.people for values with (modulus 3, remainder 1);");
        sql.ShouldContain("create table if not exists partitions.people_three partition of partitions.people for values with (modulus 3, remainder 2);");

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
