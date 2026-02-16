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
