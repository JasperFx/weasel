using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     SQLite does not imply NOT NULL for a primary key. Outside a WITHOUT ROWID table only
///     <c>INTEGER PRIMARY KEY</c> is safe, and only because it is a rowid alias whose NULL is
///     replaced by the next rowid rather than stored; a REAL or TEXT primary key stores the NULL.
/// </summary>
public class primary_key_null_rejection
{
    private static async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection($"Data Source={Path.GetTempFileName()};");
        await conn.OpenAsync();
        await conn.ResetSchemaAsync("main");
        return conn;
    }

    private static async Task<string?> insertNullKeyAsync(SqliteConnection conn, string table)
    {
        try
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = $"insert into {table} (id) values (null)";
            await cmd.ExecuteNonQueryAsync();
            return null;
        }
        catch (SqliteException e)
        {
            return e.Message;
        }
    }

    private static async Task createAsync(SqliteConnection conn, Table table)
    {
        var writer = new StringWriter();
        table.WriteCreateStatement(new SqliteMigrator(), writer);

        var cmd = conn.CreateCommand();
        cmd.CommandText = writer.ToString();
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task a_text_primary_key_declared_not_null_rejects_a_null()
    {
        await using var conn = await openAsync();

        var table = new Table("text_key");
        table.AddColumn("id", "TEXT").AsPrimaryKey().NotNull();
        await createAsync(conn, table);

        (await insertNullKeyAsync(conn, "text_key")).ShouldContain("NOT NULL constraint failed");
    }

    [Fact]
    public async Task a_real_primary_key_declared_not_null_rejects_a_null()
    {
        await using var conn = await openAsync();

        var table = new Table("real_key");
        table.AddColumn("id", "REAL").AsPrimaryKey().NotNull();
        await createAsync(conn, table);

        (await insertNullKeyAsync(conn, "real_key")).ShouldContain("NOT NULL constraint failed");
    }

    /// <summary>
    ///     The rowid alias keeps its own behaviour: a NULL is still turned into the next rowid
    ///     rather than rejected, so emitting NOT NULL alongside it costs nothing.
    /// </summary>
    [Fact]
    public async Task an_integer_primary_key_still_assigns_a_rowid()
    {
        await using var conn = await openAsync();

        var table = new Table("integer_key");
        table.AddColumn("id", "INTEGER").AsPrimaryKey().NotNull();
        await createAsync(conn, table);

        (await insertNullKeyAsync(conn, "integer_key")).ShouldBeNull();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "select id from integer_key";
        (await cmd.ExecuteScalarAsync()).ShouldBe(1L);
    }
}
