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
    [InlineData("us ers")]
    [InlineData("us\ters")]
    [InlineData("us\ners")]
    [InlineData("us\rers")]
    [InlineData("users\n-- the rest of this statement is now a comment")]
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
}
