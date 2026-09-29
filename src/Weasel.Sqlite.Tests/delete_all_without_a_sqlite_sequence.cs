using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests;

/// <summary>
///     Delete-all threw against a schema with no <c>AUTOINCREMENT</c> table, after already emptying
///     the tables ahead of the statement that threw.
/// </summary>
/// <remarks>
///     <para>
///         <c>sqlite_sequence</c> exists in a database only once something in it has been declared
///         <c>AUTOINCREMENT</c>. SQLite resolves table names when it prepares a statement, so
///         <c>DELETE FROM "main".sqlite_sequence</c> against a schema that has none fails with
///         <c>no such table</c> — and no <c>WHERE</c> guard can prevent it, because the guard is never
///         read. Verified: a subquery over <c>sqlite_master</c> fails identically.
///     </para>
///     <para>
///         Microsoft.Data.Sqlite prepares and steps a multi-statement <c>CommandText</c> one statement
///         at a time, so the table deletes ahead of it had already committed. The operation half
///         applied and then threw, which is why the fix belongs where the SQL is built rather than in a
///         <c>catch</c> (weasel#546).
///     </para>
/// </remarks>
public class delete_all_without_a_sqlite_sequence
{
    private static CancellationToken ct => TestContext.Current.CancellationToken;

    private static async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection("Data Source=:memory:");
        await conn.OpenAsync(ct);
        return conn;
    }

    private static async Task executeAsync(SqliteConnection conn, string sql)
    {
        await conn.CreateCommand(sql).ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> countAsync(SqliteConnection conn, string table)
    {
        return Convert.ToInt64(await conn.CreateCommand($"SELECT count(*) FROM {table}").ExecuteScalarAsync(ct));
    }

    /// <summary>
    ///     The reported failure: no table in the schema is AUTOINCREMENT, so there is no
    ///     <c>sqlite_sequence</c> to delete from, and nothing that needs doing.
    /// </summary>
    [Fact]
    public async Task a_schema_with_no_autoincrement_table_is_emptied_rather_than_throwing()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "CREATE TABLE main.records (id INTEGER PRIMARY KEY, name TEXT);");
        await executeAsync(conn, "INSERT INTO main.records (name) VALUES ('a'), ('b');");

        var sql = await new SqliteMigrator()
            .GenerateDeleteAllSqlAsync(conn, [new SqliteObjectName("main", "records")], ct: ct);

        sql.ShouldNotContain("sqlite_sequence");

        await executeAsync(conn, sql);
        (await countAsync(conn, "main.records")).ShouldBe(0);
    }

    /// <summary>
    ///     The half-applied shape, pinned directly: the statement that threw came after the table
    ///     deletes, so on master the rows were already gone when it threw.
    /// </summary>
    [Fact]
    public async Task the_connectionless_form_still_throws_after_emptying_the_table()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "CREATE TABLE main.records (id INTEGER PRIMARY KEY, name TEXT);");
        await executeAsync(conn, "INSERT INTO main.records (name) VALUES ('a'), ('b');");

        var sql = new SqliteMigrator().GenerateDeleteAllSql([new SqliteObjectName("main", "records")]);

        var failure = await Should.ThrowAsync<SqliteException>(async () => await executeAsync(conn, sql));
        failure.Message.ShouldContain("sqlite_sequence");
        (await countAsync(conn, "main.records")).ShouldBe(0, "the rows went before the throw");
    }

    /// <summary>
    ///     A schema that does have one is still reset — the fix withholds the statement, it does not
    ///     stop resetting.
    /// </summary>
    [Fact]
    public async Task a_schema_with_an_autoincrement_table_is_still_reset()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "CREATE TABLE main.records (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);");
        await executeAsync(conn, "INSERT INTO main.records (name) VALUES ('a');");

        var sql = await new SqliteMigrator()
            .GenerateDeleteAllSqlAsync(conn, [new SqliteObjectName("main", "records")], ct: ct);

        sql.ShouldContain("DELETE FROM main.sqlite_sequence WHERE name IN ('records');");

        await executeAsync(conn, sql);
        (await countAsync(conn, "main.sqlite_sequence")).ShouldBe(0);

        await executeAsync(conn, "INSERT INTO main.records (name) VALUES ('a2');");
        Convert.ToInt64(await conn.CreateCommand("SELECT MAX(id) FROM main.records").ExecuteScalarAsync(ct))
            .ShouldBe(1, "the identity was not reset");
    }

    /// <summary>
    ///     The multi-schema case the per-schema statements introduced: one schema has a sequence and
    ///     the other does not, and only the one that does gets a reset.
    /// </summary>
    [Fact]
    public async Task only_the_schema_that_has_a_sequence_is_reset()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "CREATE TABLE main.records (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);");
        await executeAsync(conn, "CREATE TABLE temp.records (id INTEGER PRIMARY KEY, name TEXT);");
        await executeAsync(conn, "INSERT INTO main.records (name) VALUES ('m');");
        await executeAsync(conn, "INSERT INTO temp.records (name) VALUES ('t');");

        var sql = await new SqliteMigrator().GenerateDeleteAllSqlAsync(
            conn,
            [new SqliteObjectName("main", "records"), new SqliteObjectName("temp", "records")],
            ct: ct);

        sql.ShouldContain("DELETE FROM main.sqlite_sequence");
        sql.ShouldNotContain("""DELETE FROM "temp".sqlite_sequence""");

        await executeAsync(conn, sql);
        (await countAsync(conn, "main.records")).ShouldBe(0);
        (await countAsync(conn, "temp.records")).ShouldBe(0);
    }

    /// <summary>
    ///     The sequence appears the first time anything is declared AUTOINCREMENT, so the answer is not
    ///     a property of the model and cannot be memoized from an earlier call.
    /// </summary>
    [Fact]
    public async Task the_probe_follows_the_database_rather_than_the_model()
    {
        await using var conn = await openAsync();
        var migrator = new SqliteMigrator();
        var tables = new DbObjectName[] { new SqliteObjectName("main", "records") };

        await executeAsync(conn, "CREATE TABLE main.records (id INTEGER PRIMARY KEY, name TEXT);");
        (await migrator.GenerateDeleteAllSqlAsync(conn, tables, ct: ct)).ShouldNotContain("sqlite_sequence");

        await executeAsync(conn, "CREATE TABLE main.later (id INTEGER PRIMARY KEY AUTOINCREMENT);");
        await executeAsync(conn, "INSERT INTO main.later DEFAULT VALUES;");

        (await migrator.GenerateDeleteAllSqlAsync(conn, tables, ct: ct)).ShouldContain("sqlite_sequence");
    }

    /// <summary>
    ///     resetIdentity: false never emitted the statement and still does not ask the database.
    /// </summary>
    [Fact]
    public async Task not_resetting_identity_needs_no_probe()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "CREATE TABLE main.records (id INTEGER PRIMARY KEY, name TEXT);");

        var sql = await new SqliteMigrator().GenerateDeleteAllSqlAsync(
            conn, [new SqliteObjectName("main", "records")], resetIdentity: false, ct: ct);

        sql.Trim().ShouldBe("DELETE FROM main.records;");
    }
}
