using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

[Collection("partitions")]
public class creating_tables_with_partitions: IntegrationContext
{
    private readonly Table theTable;

    public creating_tables_with_partitions() : base("partitions")
    {
        theTable = new Table("partitions.people");
        theTable.AddColumn<int>("id").AsPrimaryKey();
        theTable.AddColumn<string>("first_name");
        theTable.AddColumn<string>("last_name");
        theTable.AddColumn<string>("role").AsPrimaryKey();
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

        await theTable.MigrateAsync(theConnection);
    }

    [Fact]
    public async Task add_some_list_partitions()
    {
        theTable.PartitionByList("role");
        theTable.AddListPartition("admin", "admin");
        theTable.AddListPartition("super", "super");
        theTable.AddListPartition("special", "special");

        var sql = theTable.ToCreateSql(new PostgresqlMigrator());
        sql.ShouldContain("partitions.people_admin");
        sql.ShouldContain("partitions.people_super");
        sql.ShouldContain("partitions.people_special");
        sql.ShouldContain("partitions.people_default");

        await tryToCreateTable();
    }

    [Fact]
    public async Task write_sql_for_partition_by_hash()
    {
        theTable.PartitionByHash(new HashPartitioning
        {
            Columns = ["role"],
            Suffixes = ["one", "two", "three"]
        });

        var sql = theTable.ToCreateSql(new PostgresqlMigrator());

        sql.ShouldContain("PARTITION BY HASH (role)");

        sql.ShouldContain("create table partitions.people_one partition of partitions.people for values with (modulus 3, remainder 0);");
        sql.ShouldContain("create table partitions.people_two partition of partitions.people for values with (modulus 3, remainder 1);");
        sql.ShouldContain("create table partitions.people_three partition of partitions.people for values with (modulus 3, remainder 2);");


        await tryToCreateTable();
    }


    [Fact]
    public async Task add_range_partitions()
    {
        theTable.AddColumn<int>("age").AsPrimaryKey();
        theTable.PartitionByRange("age");

        theTable.AddRangePartition("twenties", 20, 29);
        theTable.AddRangePartition("thirties", 30, 39);

        var sql = theTable.ToCreateSql(new PostgresqlMigrator());
        sql.ShouldContain("partitions.people_twenties");
        sql.ShouldContain("partitions.people_thirties");
        sql.ShouldContain("partitions.people_default");

        await tryToCreateTable();
    }

    [Fact]
    public async Task write_sql_for_partition_by_range()
    {
        theTable.PartitionByRange("role");

        var sql = theTable.ToCreateSql(new PostgresqlMigrator());

        sql.ShouldContain("PARTITION BY RANGE (role)");

        await tryToCreateTable();
    }
}
