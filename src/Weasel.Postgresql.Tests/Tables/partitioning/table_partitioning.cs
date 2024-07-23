using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

[Collection("partitions")]
public class table_partitioning: IntegrationContext
{
    public table_partitioning(): base("partitions")
    {
    }

    [Fact]
    public void partition_strategy_is_none_by_default()
    {
        var table = new Table("partitions.people");
        table.Partitioning.ShouldBeNull();
    }

    [Fact]
    public async Task build_create_statement_with_partitions_by_column_expression()
    {
        await ResetSchema();

        var table = new Table("partitions.people");
        table.AddColumn<int>("id").AsPrimaryKey().PartitionByRange();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        table.ToBasicCreateTableSql().ShouldContain("PARTITION BY RANGE (id)");

        await CreateSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection))
            .ShouldBeTrue();
    }

    [Fact]
    public async Task detect_happy_path_difference_with_partitions()
    {
        await ResetSchema();

        var table = new Table("partitions.people");
        table.AddColumn<int>("id").AsPrimaryKey().PartitionByRange();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        await CreateSchemaObjectInDatabase(table);

        table.PartitionByRange("id");

        var delta = await table.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }



    [Fact]
    public async Task force_columns_in_range_partitions_to_be_primary_key()
    {
        await ResetSchema();

        var table = new Table("partitions.people");
        table.AddColumn<int>("id").AsPrimaryKey().PartitionByRange();
        table.AddColumn<DateTime>("modified").DefaultValueByExpression("now()");
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        table.Partitioning.ShouldBeOfType<RangePartitioning>()
            .Columns.Single().ShouldBe("id");

        await CreateSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection))
            .ShouldBeTrue();
    }


    [Fact]
    public async Task build_create_statement_with_partitions_by_range_expression()
    {
        await ResetSchema();

        var table = new Table("partitions.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        table.PartitionByRange("id");


        table.Partitioning.ShouldBeOfType<RangePartitioning>()
            .Columns.Single().ShouldBe("id");


        table.ToBasicCreateTableSql().ShouldContain("PARTITION BY RANGE (id)");

        await CreateSchemaObjectInDatabase(table);

        (await table.ExistsInDatabaseAsync(theConnection))
            .ShouldBeTrue();
    }

    [Fact]
    public async Task fetch_existing_table_with_range_partition()
    {
        await ResetSchema();

        var table = new Table("partitions.people");
        table.AddColumn<int>("id").AsPrimaryKey().PartitionByRange();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");
        table.AddColumn<DateTime>("last_modified");


        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);

        table.Partitioning.ShouldBeOfType<RangePartitioning>()
            .Columns.Single().ShouldBe("id");
    }
}
