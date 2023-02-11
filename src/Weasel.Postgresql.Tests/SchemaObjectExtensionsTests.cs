using System;
using System.Threading.Tasks;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests
{
    [Collection("extensions")]
    public class SchemaObjectExtensionsTests : IntegrationContext
    {
        public SchemaObjectExtensionsTests() : base("extensions")
        {
        }

        [Fact]
        public async Task ensure_schema_exists()
        {

            await ResetSchema();

            await theConnection.CreateCommand($"drop schema if exists one cascade").ExecuteNonQueryAsync();
            await theConnection.CreateCommand($"drop schema if exists two cascade").ExecuteNonQueryAsync();
            await theConnection.CreateCommand($"drop schema if exists three cascade").ExecuteNonQueryAsync();

            var schemaNames = await theConnection.ActiveSchemaNamesAsync();
            
            schemaNames.ShouldNotContain("one");
            schemaNames.ShouldNotContain("two");
            schemaNames.ShouldNotContain("three");

            await theConnection.EnsureSchemaExists("one");
            await theConnection.EnsureSchemaExists("one");
            await theConnection.EnsureSchemaExists("one");
            
            (await theConnection.ActiveSchemaNamesAsync()).ShouldContain("one");
        }

        [Fact]
        public async Task existing_tables()
        {
            await ResetSchema();

            var table1 = new Table("extensions.table1");
            table1.AddColumn<int>("id");
            var table2 = new Table("extensions.table2");
            table2.AddColumn<int>("id");
            var table3 = new Table("extensions.table3");
            table3.AddColumn<int>("id");
            var table4 = new Table("extensions.table4");
            table4.AddColumn<int>("id");

            await CreateSchemaObjectInDatabase(table1);
            await CreateSchemaObjectInDatabase(table2);
            await CreateSchemaObjectInDatabase(table3);
            await CreateSchemaObjectInDatabase(table4);

            var tables = await theConnection.ExistingTablesAsync();
            
            tables.ShouldContain(table1.Identifier);
            tables.ShouldContain(table2.Identifier);
            tables.ShouldContain(table3.Identifier);
            tables.ShouldContain(table4.Identifier);
            
            (await theConnection.ExistingTablesAsync(schemas:new string[]{"extensions"})).Count.ShouldBe(4);
        }
        
        [Fact]
        public async Task existing_tables_with_schemas()
        {
            await ResetSchema();

            var table1 = new Table("extensions.table1");
            table1.AddColumn<int>("id");
            var table2 = new Table("extensions.table2");
            table2.AddColumn<int>("id");
            var table3 = new Table("extensions.table3");
            table3.AddColumn<int>("id");
            var table4 = new Table("extensions.table4");
            table4.AddColumn<int>("id");

            await CreateSchemaObjectInDatabase(table1);
            await CreateSchemaObjectInDatabase(table2);
            await CreateSchemaObjectInDatabase(table3);
            await CreateSchemaObjectInDatabase(table4);

            var tables = await theConnection.ExistingTablesAsync(schemas:new string[]{"extensions"});
            
            tables.ShouldContain(table1.Identifier);
            tables.ShouldContain(table2.Identifier);
            tables.ShouldContain(table3.Identifier);
            tables.ShouldContain(table4.Identifier);
            
            (await theConnection.ExistingTablesAsync(schemas:new string[]{"extensions"})).Count.ShouldBe(4);
        }
        
                
        [Fact]
        public async Task existing_tables_with_schemas_and_name_pattern()
        {
            await ResetSchema();

            var table1 = new Table("extensions.mt_table1");
            table1.AddColumn<int>("id");
            var table2 = new Table("extensions.table2");
            table2.AddColumn<int>("id");
            var table3 = new Table("extensions.mt_table3");
            table3.AddColumn<int>("id");
            var table4 = new Table("extensions.table4");
            table4.AddColumn<int>("id");

            await CreateSchemaObjectInDatabase(table1);
            await CreateSchemaObjectInDatabase(table2);
            await CreateSchemaObjectInDatabase(table3);
            await CreateSchemaObjectInDatabase(table4);

            var tables = await theConnection.ExistingTablesAsync("mt_%",schemas:new string[]{"extensions"});
            
            tables.ShouldContain(table1.Identifier);
            tables.ShouldContain(table3.Identifier);
            
            tables.Count.ShouldBe(2);
            
        }
    }
}