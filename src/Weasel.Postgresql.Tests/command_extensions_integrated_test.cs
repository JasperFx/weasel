using System.Data.Common;
using System.Threading.Tasks;
using Castle.Components.DictionaryAdapter;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Postgresql.Tests
{
    [Collection("general")]
    public class command_extensions_integrated_test : IntegrationContext
    {
        public command_extensions_integrated_test() : base("general")
        {
        }

        [Fact]
        public async Task execute_once()
        {
            var table = new Table("general.thing");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("tag");
            table.AddColumn<int>("age");

            await ResetSchema();

            await CreateSchemaObjectInDatabase(table);

            var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
            DbCommand command = conn.CreateCommand("insert into general.thing (id, tag, age) values (@id, @tag, @age)")
                .With("id", 3).With("tag", "Toodles").With("age", 5);
            
            await command.ExecuteOnce();
            
            
            using var reader = await theConnection.CreateCommand("select id, tag, age from general.thing")
                .ExecuteReaderAsync();

            await reader.ReadAsync();
            
            (await reader.GetFieldValueAsync<int>(0)).ShouldBe(3);
            (await reader.GetFieldValueAsync<string>(1)).ShouldBe("Toodles");
            (await reader.GetFieldValueAsync<int>(2)).ShouldBe(5);
        }
    }
}