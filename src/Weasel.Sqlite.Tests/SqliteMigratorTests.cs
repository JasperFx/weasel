using Shouldly;
using Weasel.Core;
using Weasel.Sqlite;
using Xunit;

namespace Weasel.Sqlite.Tests;

public class SqliteMigratorTests
{
    [Fact]
    public void default_table_creation_is_create_if_not_exists()
    {
        var migrator = new SqliteMigrator();
        migrator.TableCreation.ShouldBe(CreationStyle.CreateIfNotExists);
    }

    [Fact]
    public void can_set_table_creation_to_drop_then_create()
    {
        var migrator = new SqliteMigrator { TableCreation = CreationStyle.DropThenCreate };
        migrator.TableCreation.ShouldBe(CreationStyle.DropThenCreate);
    }

    [Fact]
    public void default_formatting_is_pretty()
    {
        var migrator = new SqliteMigrator();
        migrator.Formatting.ShouldBe(SqlFormatting.Pretty);
    }

    [Fact]
    public void can_set_formatting_to_concise()
    {
        var migrator = new SqliteMigrator { Formatting = SqlFormatting.Concise };
        migrator.Formatting.ShouldBe(SqlFormatting.Concise);
    }

    [Fact]
    public void write_transactional_script()
    {
        var migrator = new SqliteMigrator();
        var writer = new StringWriter();

        migrator.WriteScript(writer, (m, w) =>
        {
            w.WriteLine("CREATE TABLE users (id INTEGER PRIMARY KEY);");
            w.WriteLine("CREATE TABLE posts (id INTEGER PRIMARY KEY);");
        });

        var script = writer.ToString();

        script.ShouldContain("BEGIN TRANSACTION;");
        script.ShouldContain("CREATE TABLE users");
        script.ShouldContain("CREATE TABLE posts");
        script.ShouldContain("COMMIT;");
    }

    [Fact]
    public void execute_script_line()
    {
        var migrator = new SqliteMigrator();
        var command = migrator.ToExecuteScriptLine("migration_001.sql");

        command.ShouldBe(".read migration_001.sql");
    }

    [Fact]
    public void write_schema_creation_does_nothing()
    {
        var migrator = new SqliteMigrator();
        var writer = new StringWriter();

        migrator.WriteSchemaCreationSql(new[] { "mydb", "main" }, writer);

        var script = writer.ToString();
        // WriteSchemaCreationSql is a no-op - schema attachment is handled at connection level
        script.Trim().ShouldBeEmpty();
    }

    [Fact]
    public void assert_valid_identifier_accepts_normal_names()
    {
        var migrator = new SqliteMigrator();

        Should.NotThrow(() => migrator.AssertValidIdentifier("users"));
        Should.NotThrow(() => migrator.AssertValidIdentifier("my_table"));
        Should.NotThrow(() => migrator.AssertValidIdentifier("Table123"));
    }

    [Fact]
    public void assert_valid_identifier_rejects_empty()
    {
        var migrator = new SqliteMigrator();

        Should.Throw<InvalidOperationException>(() => migrator.AssertValidIdentifier(""));
        Should.Throw<InvalidOperationException>(() => migrator.AssertValidIdentifier("   "));
    }

    [Fact]
    public void assert_valid_identifier_rejects_too_long()
    {
        var migrator = new SqliteMigrator();
        var longName = new string('a', 300);

        Should.Throw<InvalidOperationException>(() => migrator.AssertValidIdentifier(longName));
    }

    /// <summary>
    ///     weasel#416. AssertValidIdentifier is the only identifier check in the stack, so it has to reject
    ///     the characters that let a name escape the statement it is written into. A <c>"</c> closes a
    ///     quoted identifier -- SchemaUtils.QuoteName doubles an embedded quote but only quotes at all for
    ///     keywords, spaces, dashes and leading digits, so an ordinary-looking name carrying one is written
    ///     out raw -- a <c>'</c> closes the string literal the introspection path interpolates names into
    ///     (<c>pragma_table_info('...')</c>), and a <c>;</c> starts a new statement.
    /// </summary>
    [Theory]
    [InlineData("users\"", "a trailing double quote")]
    [InlineData("\"users", "a leading double quote")]
    [InlineData("us\"ers", "an embedded double quote")]
    [InlineData("\"users\"", "a fully quote-wrapped name")]
    [InlineData("users\"; drop table users; --", "a quote-and-semicolon payload")]
    [InlineData("users'", "a trailing single quote")]
    [InlineData("us'ers", "an embedded single quote")]
    [InlineData("users'; drop table users; --", "a literal-breaking payload")]
    [InlineData("users;", "a trailing semicolon")]
    [InlineData("us;ers", "an embedded semicolon")]
    public void assert_identifier_rejects_quote_and_semicolon(string name, string description)
    {
        var migrator = new SqliteMigrator();

        Should.Throw<InvalidOperationException>(
            () => migrator.AssertValidIdentifier(name),
            $"Expected {description} to be rejected");
    }

    /// <summary>
    ///     Only null and all-whitespace names used to be checked, so a name could still carry an interior
    ///     newline and smuggle a '--' comment into the statement.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("us\ters")]
    [InlineData("us\ners")]
    [InlineData("us\rers")]
    [InlineData("users\n-- the rest of this statement is now a comment")]
    /// <remarks>
    ///     An interior space is deliberately absent from this list as of weasel#448: every provider
    ///     quotes for shape now (weasel#447), so "unit price" is safe and is somebody's real legacy
    ///     column. A line break or tab still is not — it can smuggle a '--' comment into the statement.
    /// </remarks>
    public void assert_identifier_rejects_null_and_interior_whitespace(string? name)
    {
        var migrator = new SqliteMigrator();

        Should.Throw<InvalidOperationException>(() => migrator.AssertValidIdentifier(name!));
    }

    [Fact]
    public void invalid_identifier_message_says_which_rule_was_broken()
    {
        var migrator = new SqliteMigrator();

        var ex = Should.Throw<InvalidOperationException>(() => migrator.AssertValidIdentifier("us\"ers"));

        ex.Message.ShouldContain("us\"ers");
        ex.Message.ShouldContain("double quote");
    }

    /// <summary>
    ///     Names Weasel and its consumers actually generate must keep working -- the tightening is aimed at
    ///     a handful of characters, not at narrowing the identifier grammar.
    /// </summary>
    [Theory]
    [InlineData("mt_doc_user")]
    [InlineData("mt_stream")]
    [InlineData("mt_doc_user_hilo")]
    [InlineData("users$1")]
    [InlineData("_leading_underscore")]
    [InlineData("MixedCaseName")]
    [InlineData("naïve_café")]
    [InlineData("table-with-dashes")]
    [InlineData("mt_doc_target.p_tenant_one")]
    public void assert_identifier_still_accepts_ordinary_names(string name)
    {
        Should.NotThrow(() => new SqliteMigrator().AssertValidIdentifier(name));
    }

    [Fact]
    public async Task ensure_database_exists_is_noop_for_memory()
    {
        var migrator = new SqliteMigrator();
        await using var connection = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        // Should not throw - SQLite databases are auto-created
        await migrator.EnsureDatabaseExistsAsync(connection);
    }

    [Fact]
    public async Task ensure_database_exists_is_noop_for_file()
    {
        var migrator = new SqliteMigrator();
        var tempFile = Path.Combine(Path.GetTempPath(), $"weasel_test_{Guid.NewGuid():N}.db");

        try
        {
            await using var connection = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={tempFile}");

            // Should not throw - SQLite databases are auto-created
            await migrator.EnsureDatabaseExistsAsync(connection);
        }
        finally
        {
            if (File.Exists(tempFile))
            {
                File.Delete(tempFile);
            }
        }
    }

    [Fact]
    public void delete_all_sql_qualifies_a_table_outside_main()
    {
        var sql = new SqliteMigrator()
            .GenerateDeleteAllSql([new SqliteObjectName("temp", "records")], resetIdentity: false);

        sql.Trim().ShouldBe("""DELETE FROM "temp".records;""");
    }

    [Fact]
    public void delete_all_sql_qualifies_main_explicitly()
    {
        var sql = new SqliteMigrator()
            .GenerateDeleteAllSql([new SqliteObjectName("main", "records")], resetIdentity: false);

        sql.Trim().ShouldBe("DELETE FROM main.records;");
    }

    [Fact]
    public void delete_all_sql_resets_identity_in_each_owning_schema()
    {
        var sql = new SqliteMigrator().GenerateDeleteAllSql([
            new SqliteObjectName("main", "records"),
            new SqliteObjectName("aux", "records"),
            new SqliteObjectName("aux", "events")
        ]);

        sql.ShouldContain("DELETE FROM main.sqlite_sequence WHERE name IN ('records');");
        sql.ShouldContain("DELETE FROM aux.sqlite_sequence WHERE name IN ('records', 'events');");
    }

    [Fact]
    public async Task delete_all_data_empties_main_even_when_a_temp_table_shadows_it()
    {
        await using var conn = await openMemoryConnection();
        await execute(conn, "CREATE TABLE main.records (id INTEGER PRIMARY KEY, name TEXT);");
        await execute(conn, "CREATE TABLE temp.records (id INTEGER PRIMARY KEY, name TEXT);");
        await execute(conn, "INSERT INTO main.records (name) VALUES ('m');");
        await execute(conn, "INSERT INTO temp.records (name) VALUES ('t');");

        var sql = new SqliteMigrator()
            .GenerateDeleteAllSql([new SqliteObjectName("main", "records")], resetIdentity: false);
        await execute(conn, sql);

        (await count(conn, "main.records")).ShouldBe(0);
        (await count(conn, "temp.records")).ShouldBe(1);
    }

    [Fact]
    public async Task delete_all_data_resets_mains_autoincrement_mark_even_when_temp_shadows_it()
    {
        await using var conn = await openMemoryConnection();
        await execute(conn, "CREATE TABLE main.records (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);");
        await execute(conn, "CREATE TABLE temp.records (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);");
        await execute(conn, "INSERT INTO main.records (name) VALUES ('m');");
        await execute(conn, "INSERT INTO temp.records (name) VALUES ('t');");

        var sql = new SqliteMigrator().GenerateDeleteAllSql([new SqliteObjectName("main", "records")]);
        await execute(conn, sql);

        (await count(conn, "main.sqlite_sequence")).ShouldBe(0);
        (await count(conn, "temp.sqlite_sequence")).ShouldBe(1);

        await execute(conn, "INSERT INTO main.records (name) VALUES ('m2');");
        await execute(conn, "INSERT INTO temp.records (name) VALUES ('t2');");

        (await scalar(conn, "SELECT MAX(id) FROM main.records")).ShouldBe(1L);
        (await scalar(conn, "SELECT MAX(id) FROM temp.records")).ShouldBe(2L);
    }

    [Fact]
    public async Task delete_all_data_empties_an_attached_database_and_leaves_main_alone()
    {
        await using var conn = await openMemoryConnection();
        await execute(conn, "ATTACH DATABASE ':memory:' AS aux;");
        await execute(conn, "CREATE TABLE main.records (id INTEGER PRIMARY KEY, name TEXT);");
        await execute(conn, "CREATE TABLE aux.records (id INTEGER PRIMARY KEY, name TEXT);");
        await execute(conn, "INSERT INTO main.records (name) VALUES ('m');");
        await execute(conn, "INSERT INTO aux.records (name) VALUES ('a');");

        var sql = new SqliteMigrator()
            .GenerateDeleteAllSql([new SqliteObjectName("aux", "records")], resetIdentity: false);
        await execute(conn, sql);

        (await count(conn, "aux.records")).ShouldBe(0);
        (await count(conn, "main.records")).ShouldBe(1);
    }

    [Fact]
    public async Task delete_all_data_resets_the_autoincrement_mark_of_the_named_schema_only()
    {
        await using var conn = await openMemoryConnection();
        await execute(conn, "ATTACH DATABASE ':memory:' AS aux;");
        await execute(conn, "CREATE TABLE main.records (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);");
        await execute(conn, "CREATE TABLE aux.records (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);");
        await execute(conn, "INSERT INTO main.records (name) VALUES ('m');");
        await execute(conn, "INSERT INTO aux.records (name) VALUES ('a');");

        var sql = new SqliteMigrator().GenerateDeleteAllSql([new SqliteObjectName("aux", "records")]);
        await execute(conn, sql);

        (await count(conn, "aux.sqlite_sequence")).ShouldBe(0);
        (await count(conn, "main.sqlite_sequence")).ShouldBe(1);

        await execute(conn, "INSERT INTO aux.records (name) VALUES ('a2');");
        await execute(conn, "INSERT INTO main.records (name) VALUES ('m2');");

        (await scalar(conn, "SELECT MAX(id) FROM aux.records")).ShouldBe(1L);
        (await scalar(conn, "SELECT MAX(id) FROM main.records")).ShouldBe(2L);
    }

    private static async Task<Microsoft.Data.Sqlite.SqliteConnection> openMemoryConnection()
    {
        var conn = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
        await conn.OpenAsync();
        return conn;
    }

    private static async Task execute(Microsoft.Data.Sqlite.SqliteConnection conn, string sql)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static Task<object?> scalar(Microsoft.Data.Sqlite.SqliteConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalarAsync();
    }

    private static async Task<long> count(Microsoft.Data.Sqlite.SqliteConnection conn, string table)
    {
        return (long)(await scalar(conn, $"SELECT COUNT(*) FROM {table}"))!;
    }
}
