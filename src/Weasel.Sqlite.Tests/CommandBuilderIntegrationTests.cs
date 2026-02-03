using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests;

[Collection("integration")]
public class CommandBuilderIntegrationTests
{
    [Fact]
    public async Task use_parameters_to_query_by_anonymous_type()
    {
        await using var connection = await OpenConnectionAsync();
        await CreateThingTable(connection);

        var builder = new CommandBuilder();
        builder.Append("insert into thing (id, tag, age) values (@id, @tag, @age)");
        builder.AddParameters(new { id = 3, tag = "Toodles", age = 5 });

        await connection.ExecuteNonQueryAsync(builder);

        await using var reader = await connection.CreateCommand("select id, tag, age from thing")
            .ExecuteReaderAsync();

        await reader.ReadAsync();

        (await reader.GetFieldValueAsync<int>(0)).ShouldBe(3);
        (await reader.GetFieldValueAsync<string>(1)).ShouldBe("Toodles");
        (await reader.GetFieldValueAsync<int>(2)).ShouldBe(5);
    }

    [Fact]
    public async Task use_parameters_to_query_by_dictionary_string_object()
    {
        await using var connection = await OpenConnectionAsync();
        await CreateThingTable(connection);

        var builder = new CommandBuilder();
        builder.Append("insert into thing (id, tag, age) values (@id, @tag, @age)");
        var parameters = new Dictionary<string, object?> { { "id", 3 }, { "tag", "Toodles" }, { "age", 5 } };
        builder.AddParameters(parameters);

        await connection.ExecuteNonQueryAsync(builder);

        await using var reader = await connection.CreateCommand("select id, tag, age from thing")
            .ExecuteReaderAsync();

        await reader.ReadAsync();

        (await reader.GetFieldValueAsync<int>(0)).ShouldBe(3);
        (await reader.GetFieldValueAsync<string>(1)).ShouldBe("Toodles");
        (await reader.GetFieldValueAsync<int>(2)).ShouldBe(5);
    }

    [Fact]
    public async Task use_parameters_to_query_by_anonymous_type_with_generic_db_builder()
    {
        await using var connection = await OpenConnectionAsync();
        await CreateThingTable(connection);

        var builder = new DbCommandBuilder(connection);
        builder.Append("insert into thing (id, tag, age) values (@id, @tag, @age)");
        builder.AddParameters(new { id = 3, tag = "Toodles", age = 5 });

        await connection.ExecuteNonQueryAsync(builder);

        await using var reader = await connection.CreateCommand("select id, tag, age from thing")
            .ExecuteReaderAsync();

        await reader.ReadAsync();

        (await reader.GetFieldValueAsync<int>(0)).ShouldBe(3);
        (await reader.GetFieldValueAsync<string>(1)).ShouldBe("Toodles");
        (await reader.GetFieldValueAsync<int>(2)).ShouldBe(5);
    }

    [Fact]
    public async Task fetch_list()
    {
        await using var connection = await OpenConnectionAsync();
        await CreateTagTable(connection);

        await connection.CreateCommand("insert into thing (id, tag) values (@id, @tag)")
            .With("id", 1)
            .With("tag", "one")
            .ExecuteNonQueryAsync();

        await connection.CreateCommand("insert into thing (id, tag) values (@id, @tag)")
            .With("id", 2)
            .With("tag", "two")
            .ExecuteNonQueryAsync();

        await connection.CreateCommand("insert into thing (id, tag) values (@id, @tag)")
            .With("id", 3)
            .With("tag", "three")
            .ExecuteNonQueryAsync();

        var builder = new CommandBuilder();
        builder.Append("select id, tag from thing order by id");

        var things = await connection.FetchListAsync(builder, async (reader, ct) =>
        {
            var thing = new Thing
            {
                Id = await reader.GetFieldValueAsync<int>(0, ct),
                Tag = await reader.GetFieldValueAsync<string>(1, ct)
            };

            return thing;
        });

        things.ElementAt(0).Tag.ShouldBe("one");
        things.ElementAt(0).Id.ShouldBe(1);
        things.ElementAt(1).Tag.ShouldBe("two");
        things.ElementAt(2).Tag.ShouldBe("three");
        things.Count.ShouldBe(3);
    }

    [Fact]
    public async Task add_named_parameter()
    {
        await using var connection = await OpenConnectionAsync();
        await CreateDataTable(connection);

        var builder = new CommandBuilder();
        builder.Append(
            "insert into thing (id, tag, age, rate, sequence, is_done) values (@id, @tag, @age, @rate, @sequence, @done)");
        builder.AddNamedParameter("id", 3);
        builder.AddNamedParameter("tag", "toodles");
        builder.AddNamedParameter("age", 5);
        builder.AddNamedParameter("rate", 1.1);
        builder.AddNamedParameter("sequence", 100L);
        builder.AddNamedParameter("done", true);

        await connection.ExecuteNonQueryAsync(builder);

        await using var reader = await connection
            .CreateCommand("select id, tag, age, rate, sequence, is_done from thing")
            .ExecuteReaderAsync();

        await reader.ReadAsync();

        (await reader.GetFieldValueAsync<int>(0)).ShouldBe(3);
        (await reader.GetFieldValueAsync<string>(1)).ShouldBe("toodles");
        (await reader.GetFieldValueAsync<int>(2)).ShouldBe(5);
        (await reader.GetFieldValueAsync<double>(3)).ShouldBe(1.1);
        (await reader.GetFieldValueAsync<long>(4)).ShouldBe(100L);
        (await reader.GetFieldValueAsync<bool>(5)).ShouldBe(true);
    }

    private static async Task<SqliteConnection> OpenConnectionAsync()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        return connection;
    }

    private static Task CreateThingTable(SqliteConnection connection)
    {
        var table = new Table("thing");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("tag");
        table.AddColumn<int>("age");

        return CreateTable(connection, table);
    }

    private static Task CreateTagTable(SqliteConnection connection)
    {
        var table = new Table("thing");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("tag");

        return CreateTable(connection, table);
    }

    private static Task CreateDataTable(SqliteConnection connection)
    {
        var table = new Table("thing");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("tag");
        table.AddColumn<int>("age");
        table.AddColumn<double>("rate");
        table.AddColumn<long>("sequence");
        table.AddColumn<bool>("is_done");

        return CreateTable(connection, table);
    }

    private static Task CreateTable(SqliteConnection connection, Table table)
    {
        var migrator = new SqliteMigrator();
        var writer = new StringWriter();
        table.WriteCreateStatement(migrator, writer);

        return connection.CreateCommand(writer.ToString()).ExecuteNonQueryAsync();
    }

    public class Thing
    {
        public int Id { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
