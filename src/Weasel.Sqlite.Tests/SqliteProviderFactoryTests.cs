using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests;

/// <summary>
/// Tests for SqliteHelper factory methods.
/// </summary>
public class SqliteProviderFactoryTests
{
    [Fact]
    public async Task can_create_connection_with_defaults()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.db");

        try
        {
            await using var connection = await SqliteHelper.CreateConnectionAsync(
                $"Data Source={tempFile}"
            );

            connection.State.ShouldBe(System.Data.ConnectionState.Open);

            // Verify defaults were applied
            var cmd = connection.CreateCommand();
            cmd.CommandText = "PRAGMA journal_mode";
            var mode = await cmd.ExecuteScalarAsync();
            mode.ToString()!.ToUpperInvariant().ShouldBe("WAL");
        }
        finally
        {
            if (File.Exists(tempFile)) try { File.Delete(tempFile); } catch { }
        }
    }

    [Fact]
    public async Task can_create_connection_with_custom_pragmas()
    {
        await using var connection = await SqliteHelper.CreateConnectionAsync(
            "Data Source=:memory:",
            configurePragmas: settings =>
            {
                settings.JournalMode = JournalMode.MEMORY;
                settings.ForeignKeys = true;
            }
        );

        // Verify settings applied
        var cmd = connection.CreateCommand();
        cmd.CommandText = "PRAGMA journal_mode";
        var mode = await cmd.ExecuteScalarAsync();
        mode.ToString()!.ToUpperInvariant().ShouldBe("MEMORY");
    }

    [Fact]
    public void can_create_migrator()
    {
        var migrator = SqliteHelper.CreateMigrator();

        migrator.ShouldNotBeNull();
        migrator.Provider.ShouldBe(SqliteProvider.Instance);
    }

    [Fact]
    public async Task complete_workflow_example()
    {
        await using var connection = await SqliteHelper.CreateConnectionAsync(
            "Data Source=:memory:"
        );

        // Create migrator
        var migrator = SqliteHelper.CreateMigrator();

        // Define tables in main schema
        var usersTable = new Table("users");
        usersTable.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        usersTable.AddColumn<string>("name").NotNull();

        var eventsTable = new Table("events");
        eventsTable.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        eventsTable.AddColumn<int>("user_id").NotNull();
        eventsTable.AddColumn<string>("event_type").NotNull();

        // Generate and execute DDL
        var writer = new StringWriter();
        usersTable.WriteCreateStatement(migrator, writer);
        writer.WriteLine();
        eventsTable.WriteCreateStatement(migrator, writer);

        var cmd = connection.CreateCommand();
        cmd.CommandText = writer.ToString();
        await cmd.ExecuteNonQueryAsync();

        // Use the database
        var insertUser = connection.CreateCommand();
        insertUser.CommandText = "INSERT INTO users (name) VALUES ('Alice')";
        await insertUser.ExecuteNonQueryAsync();

        var insertEvent = connection.CreateCommand();
        insertEvent.CommandText = "INSERT INTO events (user_id, event_type) VALUES (1, 'login')";
        await insertEvent.ExecuteNonQueryAsync();

        // Query data
        var query = connection.CreateCommand();
        query.CommandText = @"
            SELECT u.name, e.event_type
            FROM users u
            JOIN events e ON u.id = e.user_id";

        await using var reader = await query.ExecuteReaderAsync();
        reader.Read().ShouldBeTrue();
        reader.GetString(0).ShouldBe("Alice");
        reader.GetString(1).ShouldBe("login");
    }
}
