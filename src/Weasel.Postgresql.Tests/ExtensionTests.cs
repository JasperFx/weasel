using System.Threading.Tasks;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Postgresql.Tests
{
    [Collection("extensions")]
    public class ExtensionTests: IntegrationContext
    {
        public ExtensionTests(): base("extensions")
        {
        }

        [Fact]
        public async Task can_create_extension()
        {
            await ResetSchema();
            var extension = new Extension("plv8");
            await DropSchemaObjectInDatabase(extension);


            var migration = await SchemaMigration.DetermineAsync(theConnection, extension);

            migration.Difference.ShouldBe(SchemaPatchDifference.Create);

            await this.CreateSchemaObjectInDatabase(extension);


            migration = await SchemaMigration.DetermineAsync(theConnection, extension);
            migration.Difference.ShouldBe(SchemaPatchDifference.None);
        }
    }
}
