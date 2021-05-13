using System;
using System.Linq;
using System.Threading.Tasks;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables
{
    [Collection("partitions")]
    public class table_partitioning : IntegrationContext
    {
        public table_partitioning() : base("partitions")
        {
        }

        [Fact]
        public void partition_strategy_is_none_by_default()
        {
            var table = new Table("partitions.people");
            table.PartitionStrategy.ShouldBe(PartitionStrategy.None);
        }

        [Fact]
        public async Task build_create_statement_with_partitions_by_column_expression()
        {
            await ResetSchema();
            
            var table = new Table("partitions.people");
            table.AddColumn<int>("id").AsPrimaryKey().PartitionByRange();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");
            
            
            
            table.PartitionExpressions.Single().ShouldBe("id");
            table.PartitionStrategy.ShouldBe(PartitionStrategy.Range);
            
            
            table.ToBasicCreateTableSql().ShouldContain("PARTITION BY RANGE (id)");

            await CreateSchemaObjectInDatabase(table);

            (await table.ExistsInDatabase(theConnection))
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

            var delta = await table.FindDelta(theConnection);
            
            delta.Difference.ShouldBe(SchemaPatchDifference.None);


        }


        [Fact]
        public async Task detect_difference_when_new_partition_is_found()
        {
            await ResetSchema();
            
            var table = new Table("partitions.people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");
            
            await CreateSchemaObjectInDatabase(table);
            
            table.PartitionByRange("id");

            var delta = await table.FindDelta(theConnection);
            
            delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);


        }
        

        
        
        [Fact]
        public async Task force_columns_in_range_partitions_to_be_primary_key()
        {
            await ResetSchema();
            
            var table = new Table("partitions.people");
            table.AddColumn<int>("id").AsPrimaryKey().PartitionByRange();
            table.AddColumn<DateTime>("modified").DefaultValueByExpression("now()").PartitionByRange();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");
            
            
            
            table.PartitionExpressions.ShouldContain("modified");
            table.PartitionStrategy.ShouldBe(PartitionStrategy.Range);
            table.PrimaryKeyColumns.ShouldContain("modified");
            

            await CreateSchemaObjectInDatabase(table);

            (await table.ExistsInDatabase(theConnection))
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
            
            
            
            table.PartitionExpressions.Single().ShouldBe("id");
            table.PartitionStrategy.ShouldBe(PartitionStrategy.Range);
            
            
            table.ToBasicCreateTableSql().ShouldContain("PARTITION BY RANGE (id)");

            await CreateSchemaObjectInDatabase(table);

            (await table.ExistsInDatabase(theConnection))
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

            var existing = await table.FetchExisting(theConnection);
            
            existing.PartitionExpressions.Count.ShouldBe(1);
            existing.PartitionExpressions.ShouldContain("id");
            existing.PartitionStrategy.ShouldBe(PartitionStrategy.Range);
        }
        

    }
}