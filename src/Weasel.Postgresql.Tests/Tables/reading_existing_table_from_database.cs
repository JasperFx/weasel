using System.Threading.Tasks;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables
{
    [Collection("tables")]
    public class reading_existing_table_from_database : IntegrationContext
    {
        public reading_existing_table_from_database() : base("tables")
        {
        }

        [Fact]
        public async Task read_columns()
        {
            await theConnection.OpenAsync();

            await theConnection.ResetSchema("tables");
            
            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");

            await CreateSchemaObjectInDatabase(table);

            var existing = await table.FetchExisting(theConnection);

            existing.ShouldNotBeNull();
            existing.Identifier.ShouldBe(table.Identifier);
            existing.Columns.Count.ShouldBe(table.Columns.Count);

            for (int i = 0; i < table.Columns.Count; i++)
            {
                existing.Columns[i].Name.ShouldBe(table.Columns[i].Name);
                var existingType = existing.Columns[i].Type;
                var tableType = table.Columns[i].Type;
                
                TypeMappings.ConvertSynonyms(existingType)
                    .ShouldBe(TypeMappings.ConvertSynonyms(tableType));

            }
            
            
        }
    }
}