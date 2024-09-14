using JasperFx.Core.Reflection;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

[Collection("partitions")]
public class list_partitions : IntegrationContext
{
    private readonly Table theTable;

    public list_partitions() : base("partitions")
    {
        theTable = new Table("partitions.people");
        theTable.AddColumn<int>("id").AsPrimaryKey();
        theTable.AddColumn<string>("first_name");
        theTable.AddColumn<string>("last_name");
        theTable.AddColumn<string>("role")
            .PartitionByListValues()
            .AddPartition("admin", "admin")
            .AddPartition("super", "super")
            .AddPartition("special", "special"); ;
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
    public void parse_list_partition_by_expression_with_single_value()
    {
        var partition = ListPartition.Parse(new DbObjectName("projections", "people"),"people_admin", "FOR VALUES IN ('admin')");
        partition.Suffix.ShouldBe("admin");
        partition.Values.Single().ShouldBe("'admin'");
    }

    [Fact]
    public void parse_list_partition_by_expression_with_multiple_values()
    {
        var partition = ListPartition.Parse(new DbObjectName("projections", "people"),"people_admin", "FOR VALUES IN ('admin', 'super_user')");
        partition.Suffix.ShouldBe("admin");
        partition.Values.ShouldBe(["'admin'", "'super_user'"]);
    }

    [Fact]
    public async Task can_create_table()
    {
        var sql = theTable.ToCreateSql(new PostgresqlMigrator());
        sql.ShouldContain("partitions.people_admin");
        sql.ShouldContain("partitions.people_super");
        sql.ShouldContain("partitions.people_special");
        sql.ShouldContain("partitions.people_default");

        await tryToCreateTable();
    }

    [Fact]
    public async Task can_create_table_with_default_partition_off()
    {
        theTable.Partitioning.As<ListPartitioning>().EnableDefaultPartition = false;
        var sql = theTable.ToCreateSql(new PostgresqlMigrator());
        sql.ShouldContain("partitions.people_admin");
        sql.ShouldContain("partitions.people_super");
        sql.ShouldContain("partitions.people_special");
        sql.ShouldNotContain("partitions.people_default");

        await tryToCreateTable();
    }

    [Fact]
    public async Task fetch_the_existing_table()
    {
        await tryToCreateTable();

        var existing = await tryToFetchExisting();

        var partitioning = existing.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.Columns.Single().ShouldBe("role");

        partitioning.Partitions.Count.ShouldBe(3);
        partitioning.Partitions.ShouldContain(new ListPartition("admin", "'admin'"));
        partitioning.Partitions.ShouldContain(new ListPartition("super", "'super'"));
        partitioning.Partitions.ShouldContain(new ListPartition("special", "'special'"));

        partitioning.HasExistingDefault.ShouldBeTrue();
    }

    [Fact]
    public async Task fetch_the_existing_table_when_default_partition_is_off()
    {
        theTable.Partitioning.As<ListPartitioning>().EnableDefaultPartition = false;
        await tryToCreateTable();

        var existing = await tryToFetchExisting();

        var partitioning = existing.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.Columns.Single().ShouldBe("role");

        partitioning.Partitions.Count.ShouldBe(3);
        partitioning.Partitions.ShouldContain(new ListPartition("admin", "'admin'"));
        partitioning.Partitions.ShouldContain(new ListPartition("super", "'super'"));
        partitioning.Partitions.ShouldContain(new ListPartition("special", "'special'"));

        partitioning.HasExistingDefault.ShouldBeFalse();
    }
}
