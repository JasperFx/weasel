using System.Linq;
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

        [Fact]
        public async Task read_single_pk_field()
        {
            await theConnection.OpenAsync();

            await theConnection.ResetSchema("tables");
            
            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");

            await CreateSchemaObjectInDatabase(table);

            var existing = await table.FetchExisting(theConnection);
            
            existing.PrimaryKeyColumns.Single()
                .ShouldBe("id");
        }
        
        [Fact]
        public async Task read_multi_pk_fields()
        {
            await theConnection.OpenAsync();

            await theConnection.ResetSchema("tables");
            
            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("tenant_id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");

            await CreateSchemaObjectInDatabase(table);

            var existing = await table.FetchExisting(theConnection);
            
            existing.PrimaryKeyColumns.OrderBy(x => x)
                .ShouldBe(new []{"id", "tenant_id"});
        }

        [Fact]
        public async Task read_indexes_and_foreign_keys()
        {
            await theConnection.OpenAsync();

            await theConnection.ResetSchema("tables");
            
            
            var states = new Table("states");
            states.AddColumn<int>("id").AsPrimaryKey();
            
            await CreateSchemaObjectInDatabase(states);

            
            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name").AddIndex();
            table.AddColumn<string>("last_name").AddIndex(i =>
            {
                i.IsConcurrent = true;
                i.Method = IndexMethod.hash;
            });
            
            table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

            await CreateSchemaObjectInDatabase(table);

            var existing = await table.FetchExisting(theConnection);
            
            
            
            existing.ActualIndices.Count.ShouldBe(2);
            existing.ActualIndices["idx_people_first_name"].DDL
                .ShouldBe("CREATE INDEX idx_people_first_name ON public.people USING btree (first_name)");
            
        }
    }
}