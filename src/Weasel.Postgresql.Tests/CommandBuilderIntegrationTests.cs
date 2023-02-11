using System.Linq;
using System.Threading.Tasks;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests
{
    [Collection("integration")]
    public class CommandBuilderIntegrationTests: IntegrationContext
    {
        public CommandBuilderIntegrationTests(): base("integration")
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
            builder.AddParameters(new { id = 3, tag = "Toodles", age = 5 });

            await builder.ExecuteNonQueryAsync(theConnection);

            await using var reader = await theConnection.CreateCommand("select id, tag, age from integration.thing")
                .ExecuteReaderAsync();

            await reader.ReadAsync();

            (await reader.GetFieldValueAsync<int>(0)).ShouldBe(3);
            (await reader.GetFieldValueAsync<string>(1)).ShouldBe("Toodles");
            (await reader.GetFieldValueAsync<int>(2)).ShouldBe(5);
        }

        [Fact]
        public async Task use_parameters_to_query_by_anonymous_type_by_generic_command_builder()
        {
            var table = new Table("integration.thing");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("tag");
            table.AddColumn<int>("age");

            await ResetSchema();

            await CreateSchemaObjectInDatabase(table);

            var builder = new DbCommandBuilder(theConnection);
            builder.Append("insert into integration.thing (id, tag, age) values (:id, :tag, :age)");
            builder.AddParameters(new { id = 3, tag = "Toodles", age = 5 });

            await builder.ExecuteNonQueryAsync(theConnection);

            await using var reader = await theConnection.CreateCommand("select id, tag, age from integration.thing")
                .ExecuteReaderAsync();

            await reader.ReadAsync();

            (await reader.GetFieldValueAsync<int>(0)).ShouldBe(3);
            (await reader.GetFieldValueAsync<string>(1)).ShouldBe("Toodles");
            (await reader.GetFieldValueAsync<int>(2)).ShouldBe(5);
        }

        [Fact]
        public async Task fetch_list()
        {
            var table = new Table("integration.thing");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("tag");

            await ResetSchema();

            await CreateSchemaObjectInDatabase(table);

            await theConnection.CreateCommand("insert into integration.thing (id, tag) values (:id, :tag)")
                .With("id", 1)
                .With("tag", "one")
                .ExecuteNonQueryAsync();

            await theConnection.CreateCommand("insert into integration.thing (id, tag) values (:id, :tag)")
                .With("id", 2)
                .With("tag", "two")
                .ExecuteNonQueryAsync();

            await theConnection.CreateCommand("insert into integration.thing (id, tag) values (:id, :tag)")
                .With("id", 3)
                .With("tag", "three")
                .ExecuteNonQueryAsync();


            var builder = new CommandBuilder();
            builder.Append("select id, tag from integration.thing order by id");

            var things = await builder.FetchListAsync(theConnection, async (r, ct) =>
            {
                var thing = new Thing
                {
                    id = await r.GetFieldValueAsync<int>(0), tag = await r.GetFieldValueAsync<string>(1)
                };

                return thing;
            });

            things.ElementAt(0).tag.ShouldBe("one");
            things.ElementAt(0).id.ShouldBe(1);
            things.ElementAt(1).tag.ShouldBe("two");
            things.ElementAt(2).tag.ShouldBe("three");
            things.Count.ShouldBe(3);
        }

        public class Thing
        {
            public int id { get; set; }
            public string tag { get; set; }
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
            builder.Append(
                "insert into integration.thing (id, tag, age, rate, sequence, is_done) values (:id, :tag, :age, :rate, :sequence, :done)");
            builder.AddNamedParameter("id", 3);
            builder.AddNamedParameter("tag", "toodles");
            builder.AddNamedParameter("age", 5);
            builder.AddNamedParameter("rate", 1.1);
            builder.AddNamedParameter("sequence", 100L);
            builder.AddNamedParameter("done", true);


            await builder.ExecuteNonQueryAsync(theConnection);

            await using var reader = await theConnection
                .CreateCommand("select id, tag, age, rate, sequence, is_done from integration.thing")
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
