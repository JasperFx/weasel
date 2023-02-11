using System.Linq;
using System.Threading.Tasks;
using Shouldly;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests
{
    public class SchemaObjectExtensionsTests: IntegrationContext
    {
        public SchemaObjectExtensionsTests(): base("extensions")
        {
        }

        [Fact]
        public async Task ensure_schema_exists()
        {
            await ResetSchema();

            await theConnection.DropSchemaAsync("one");
            await theConnection.DropSchemaAsync("two");
            await theConnection.DropSchemaAsync("three");


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

            var tables = await theConnection.ExistingTables();

            tables.ShouldContain(table1.Identifier);
            tables.ShouldContain(table2.Identifier);
            tables.ShouldContain(table3.Identifier);
            tables.ShouldContain(table4.Identifier);

            (await theConnection.ExistingTables()).Count(x => x.Schema == "extensions").ShouldBe(4);
        }
    }
}
