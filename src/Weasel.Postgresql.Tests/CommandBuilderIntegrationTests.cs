using System.Threading.Tasks;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests
{
    [Collection("integration")]
    public class CommandBuilderIntegrationTests : IntegrationContext
    {
        public CommandBuilderIntegrationTests() : base("integration")
        {
        }

        [Fact]
        public async Task use_parameters_to_query_by_anonymous_type()
        {
            var table = new Table("integration.thing");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("tag");
            table.AddColumn<int>("age");

            await ResetSchema();

            await CreateSchemaObjectInDatabase(table);

            var builder = new CommandBuilder();
            builder.Append("insert into integration.thing (id, tag, age) values (:id, :tag, :age)");
            builder.AddParameters(new
            {
                id = 3,
                tag = "Toodles",
                age = 5
            });

            await builder.ExecuteNonQueryAsync(theConnection);

            using var reader = await theConnection.CreateCommand("select id, tag, age from integration.thing")
                .ExecuteReaderAsync();

            await reader.ReadAsync();
            
            (await reader.GetFieldValueAsync<int>(0)).ShouldBe(3);
            (await reader.GetFieldValueAsync<string>(1)).ShouldBe("Toodles");
            (await reader.GetFieldValueAsync<int>(2)).ShouldBe(5);
        }
        
        [Fact]
        public async Task add_named_parameter()
        {
            var table = new Table("integration.thing");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("tag");
            table.AddColumn<int>("age");
            table.AddColumn<double>("rate");
            table.AddColumn<long>("sequence");
            table.AddColumn<bool>("is_done");

            await ResetSchema();

            await CreateSchemaObjectInDatabase(table);

            var builder = new CommandBuilder();
            builder.Append("insert into integration.thing (id, tag, age, rate, sequence, is_done) values (:id, :tag, :age, :rate, :sequence, :done)");
            builder.AddNamedParameter("id", 3);
            builder.AddNamedParameter("tag", "toodles");
            builder.AddNamedParameter("age", 5);
            builder.AddNamedParameter("rate", 1.1);
            builder.AddNamedParameter("sequence", 100L);
            builder.AddNamedParameter("done", true);
            

            await builder.ExecuteNonQueryAsync(theConnection);

            using var reader = await theConnection.CreateCommand("select id, tag, age, rate, sequence, is_done from integration.thing")
                .ExecuteReaderAsync();

            await reader.ReadAsync();
            
            (await reader.GetFieldValueAsync<int>(0)).ShouldBe(3);
            (await reader.GetFieldValueAsync<string>(1)).ShouldBe("toodles");
            (await reader.GetFieldValueAsync<int>(2)).ShouldBe(5);
            (await reader.GetFieldValueAsync<double>(3)).ShouldBe(1.1);
            (await reader.GetFieldValueAsync<long>(4)).ShouldBe(100L);
            (await reader.GetFieldValueAsync<bool>(5)).ShouldBe(true);
        }
    }
}