using System.Threading.Tasks;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables
{
    [Collection("tables")]
    public class creating_tables_in_database : IntegrationContext
    {
        public creating_tables_in_database() : base("tables")
        {
        }

        [Fact]
        public async Task create_table_in_the_database()
        {
            await theConnection.OpenAsync();

            await theConnection.ResetSchema("tables");
            
            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");

            await CreateSchemaObjectInDatabase(table);

            (await table.ExistsInDatabase(theConnection))
                .ShouldBeTrue();

            await theConnection.CreateCommand(
                "insert into people (id, first_name, last_name) values (1, 'Elton', 'John')")
                .ExecuteNonQueryAsync();
        }
        
        

        [Fact]
        public async Task create_then_drop()
        {
            await theConnection.OpenAsync();

            await theConnection.ResetSchema("tables");
            
            var table = new Table("people");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("first_name");
            table.AddColumn<string>("last_name");

            await CreateSchemaObjectInDatabase(table);

            await DropSchemaObjectInDatabase(table);
            
            (await table.ExistsInDatabase(theConnection))
                .ShouldBeFalse();
        }
    }
}