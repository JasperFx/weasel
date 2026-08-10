using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     Coverage for weasel#426: <c>pragma_table_info</c> does not list generated columns, so a table
///     declaring one was read back without it. The delta reported the column missing on every run,
///     emitted <c>ALTER TABLE ... ADD COLUMN</c>, and the second migration failed with
///     <c>duplicate column name</c> -- i.e. such a table never converged.
/// </summary>
[Collection("integration")]
public class GeneratedColumnDeltaTests
{
    private static async Task<SqliteConnection> OpenConnectionAsync()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        return connection;
    }

    private static Table TableWithGeneratedColumn(GeneratedColumnType type)
    {
        var table = new Table("documents");
        table.AddColumn<string>("id").AsPrimaryKey();
        table.AddColumn<string>("data").NotNull();
        table.AddColumn("name", "TEXT").GeneratedAs("json_extract(data, '$.name')", type);

        return table;
    }

    [Theory]
    [InlineData(GeneratedColumnType.Virtual)]
    [InlineData(GeneratedColumnType.Stored)]
    public async Task generated_column_is_read_back_from_the_database(GeneratedColumnType type)
    {
        await using var connection = await OpenConnectionAsync();

        var table = TableWithGeneratedColumn(type);
        await table.CreateAsync(connection);

        var existing = await table.FetchExistingAsync(connection);

        existing.ShouldNotBeNull();
        existing.Columns.Select(x => x.Name)
            .ShouldBe(["id", "data", "name"]);
    }

    [Theory]
    [InlineData(GeneratedColumnType.Virtual)]
    [InlineData(GeneratedColumnType.Stored)]
    public async Task table_with_a_generated_column_converges(GeneratedColumnType type)
    {
        await using var connection = await OpenConnectionAsync();

        var table = TableWithGeneratedColumn(type);
        await table.CreateAsync(connection);

        var delta = await table.FindDeltaAsync(connection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.Columns.Missing.ShouldBeEmpty();
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    [Fact]
    public async Task adding_a_virtual_generated_column_migrates_and_then_converges()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("documents");
        table.AddColumn<string>("id").AsPrimaryKey();
        table.AddColumn<string>("data").NotNull();

        await table.CreateAsync(connection);

        // SQLite permits ALTER TABLE ADD COLUMN for a VIRTUAL generated column, so this is an
        // incremental alter rather than a recreation.
        table.AddColumn("name", "TEXT")
            .GeneratedAs("json_extract(data, '$.name')", GeneratedColumnType.Virtual);

        var delta = await table.FindDeltaAsync(connection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.RequiresTableRecreation.ShouldBeFalse();

        await ApplyAsync(connection, delta);

        // The point of the issue: a second pass sees nothing to do rather than re-adding the column.
        var afterDelta = await table.FindDeltaAsync(connection);
        afterDelta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task adding_a_stored_generated_column_recreates_the_table_and_then_converges()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("documents");
        table.AddColumn<string>("id").AsPrimaryKey();
        table.AddColumn<string>("data").NotNull();

        await table.CreateAsync(connection);

        await connection.CreateCommand(
                "INSERT INTO \"documents\" (\"id\", \"data\") VALUES ('one', '{\"name\": \"Anne\"}');")
            .ExecuteNonQueryAsync();

        // SQLite rejects ALTER TABLE ADD COLUMN for a STORED generated column, so this has to go
        // through the recreation path.
        table.AddColumn("name", "TEXT")
            .GeneratedAs("json_extract(data, '$.name')", GeneratedColumnType.Stored);

        var delta = await table.FindDeltaAsync(connection);
        delta.RequiresTableRecreation.ShouldBeTrue();

        await ApplyAsync(connection, delta);

        var afterDelta = await table.FindDeltaAsync(connection);
        afterDelta.Difference.ShouldBe(SchemaPatchDifference.None);

        // The recreation must not have dropped the row, and the generated value has to be computed
        // from the copied 'data' column.
        var name = await connection.CreateCommand("SELECT \"name\" FROM \"documents\" WHERE \"id\" = 'one';")
            .ExecuteScalarAsync();
        name.ShouldBe("Anne");
    }

    [Fact]
    public async Task recreating_a_table_that_already_has_a_generated_column_does_not_copy_it()
    {
        await using var connection = await OpenConnectionAsync();

        var table = TableWithGeneratedColumn(GeneratedColumnType.Virtual);
        await table.CreateAsync(connection);

        await connection.CreateCommand(
                "INSERT INTO \"documents\" (\"id\", \"data\") VALUES ('one', '{\"name\": \"Anne\"}');")
            .ExecuteNonQueryAsync();

        // A foreign key change forces recreation; the generated column comes along for the ride and
        // must be left out of the INSERT ... SELECT, since SQLite refuses writes to generated columns.
        var users = new Table("users");
        users.AddColumn<string>("id").AsPrimaryKey();
        await users.CreateAsync(connection);

        table.AddColumn<string>("user_id");
        table.ForeignKeys.Add(new ForeignKey("fk_documents_users")
        {
            LinkedTable = new SqliteObjectName("users"),
            ColumnNames = ["user_id"],
            LinkedNames = ["id"]
        });

        var delta = await table.FindDeltaAsync(connection);
        delta.RequiresTableRecreation.ShouldBeTrue();

        await ApplyAsync(connection, delta);

        var name = await connection.CreateCommand("SELECT \"name\" FROM \"documents\" WHERE \"id\" = 'one';")
            .ExecuteScalarAsync();
        name.ShouldBe("Anne");
    }

    [Fact]
    public async Task the_column_query_does_not_widen_to_a_virtual_tables_hidden_columns()
    {
        await using var connection = await OpenConnectionAsync();

        // table_xinfo reports a virtual table's hidden columns -- for fts5 that is the table-name
        // column and 'rank' -- where table_info reported neither. Switching pragmas must not widen
        // what Weasel considers a column, so the query filters them back out.
        //
        // This is asserted against the query rather than through FetchExistingAsync because fts5
        // columns report an empty type, which TableColumn rejects: Weasel.Sqlite cannot introspect a
        // virtual table at all, before or after this change. That gap is its own question.
        await connection.CreateCommand("CREATE VIRTUAL TABLE search USING fts5(title, body);")
            .ExecuteNonQueryAsync();

        var queryCmd = connection.CreateCommand();
        var builder = new DbCommandBuilder(queryCmd);
        new Table("search").ConfigureQueryCommand(builder);
        builder.Compile();

        await using var reader = await queryCmd.ExecuteReaderAsync();
        await reader.NextResultAsync(); // table SQL -> columns

        var names = new List<string>();
        while (await reader.ReadAsync())
        {
            names.Add(reader.GetString(1));
        }

        names.ShouldBe(["title", "body"]);
    }

    private static async Task ApplyAsync(SqliteConnection connection, TableDelta delta)
    {
        var writer = new StringWriter();
        delta.WriteUpdate(new SqliteMigrator(), writer);

        await connection.CreateCommand(writer.ToString()).ExecuteNonQueryAsync();
    }
}
