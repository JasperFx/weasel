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

        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("partitions");

        await theTable.MigrateAsync(theConnection);
    }

    [Fact]
    public async Task write_sql_for_partition_by_hash()
    {
        theTable.PartitionByHash("role");

        var sql = theTable.ToCreateSql(new PostgresqlMigrator());

        sql.ShouldContain("PARTITION BY HASH (role)");

        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("partitions");

        await theTable.MigrateAsync(theConnection);
    }


    [Fact]
    public async Task write_sql_for_partition_by_range()
    {
        theTable.PartitionByRange("role");

        var sql = theTable.ToCreateSql(new PostgresqlMigrator());

        sql.ShouldContain("PARTITION BY RANGE (role)");

        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync("partitions");

        await theTable.MigrateAsync(theConnection);
    }
}
