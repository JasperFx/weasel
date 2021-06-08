using System.Threading.Tasks;
using Shouldly;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests
{
    [Collection("schemas")]
    public class create_and_teardown_schemas : IntegrationContext
    {
        public create_and_teardown_schemas() : base("schemas")
        {
        }

        [Fact]
        public async Task can_create_and_drop_a_schema()
        {
            await theConnection.OpenAsync();

            await theConnection.DropSchema("one");
            
            (await theConnection.ActiveSchemaNames()).ShouldNotContain("one");

            await theConnection.CreateSchema("one");
            
            var schemas = await theConnection.ActiveSchemaNames();
            
            schemas.ShouldContain("one");
        }

        [Fact]
        public async Task create_a_schema_on_the_fly_for_migrations()
        {
            await theConnection.OpenAsync();

            await theConnection.DropSchema("one");
            await theConnection.DropSchema("two");

            var table1 = new Table("one.table1");
            table1.AddColumn<string>("name").AsPrimaryKey();
            
            var table2 = new Table("two.table2");
            table2.AddColumn<string>("name").AsPrimaryKey();

            var migration = await SchemaMigration.Determine(theConnection, new ISchemaObject[] {table1, table2});
            migration.Difference.ShouldBe(SchemaPatchDifference.Create);

            await migration.ApplyAll(theConnection, new DdlRules(), AutoCreate.CreateOrUpdate);

            (await table1.FetchExisting(theConnection)).ShouldNotBeNull();
            (await table2.FetchExisting(theConnection)).ShouldNotBeNull();

        }
    }
}