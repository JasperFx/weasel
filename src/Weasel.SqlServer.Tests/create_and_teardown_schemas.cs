using System.Threading.Tasks;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests
{
    public class create_and_teardown_schemas: IntegrationContext
    {
        public create_and_teardown_schemas(): base("schemas")
        {
        }

        [Fact]
        public async Task can_create_and_drop_a_schema()
        {
            await theConnection.OpenAsync();

            await theConnection.DropSchemaAsync("one");

            (await theConnection.ActiveSchemaNamesAsync()).ShouldNotContain("one");

            await theConnection.CreateSchemaAsync("one");

            var schemas = await theConnection.ActiveSchemaNamesAsync();

            schemas.ShouldContain("one");
        }

        [Fact]
        public async Task create_a_schema_on_the_fly_for_migrations()
        {
            await theConnection.OpenAsync();

            await theConnection.DropSchemaAsync("one");
            await theConnection.DropSchemaAsync("two");

            var table1 = new Table("one.table1");
            table1.AddColumn<string>("name").AsPrimaryKey();

            var table2 = new Table("two.table2");
            table2.AddColumn<string>("name").AsPrimaryKey();

            var migration = await SchemaMigration.DetermineAsync(theConnection, new ISchemaObject[] { table1, table2 });
            migration.Difference.ShouldBe(SchemaPatchDifference.Create);

            await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

            (await table1.FetchExistingAsync(theConnection)).ShouldNotBeNull();
            (await table2.FetchExistingAsync(theConnection)).ShouldNotBeNull();
        }
    }
}
