using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests;

/// <summary>
/// Tests for SqliteDataSource connection management and PRAGMA application.
/// </summary>
public class SqliteProviderFactoryTests
{
    [Fact]
    public async Task can_create_connection_with_defaults()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.db");

        try
        {
            await using var dataSource = new SqliteDataSource($"Data Source={tempFile}");
            await using var connection = await dataSource.OpenConnectionAsync();

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
        var settings = new SqlitePragmaSettings
        {
            JournalMode = JournalMode.MEMORY,
            ForeignKeys = true
        };

        await using var dataSource = new SqliteDataSource("Data Source=:memory:", settings);
        await using var connection = await dataSource.OpenConnectionAsync();

        // Verify settings applied
        var cmd = connection.CreateCommand();
        cmd.CommandText = "PRAGMA journal_mode";
        var mode = await cmd.ExecuteScalarAsync();
        mode.ToString()!.ToUpperInvariant().ShouldBe("MEMORY");
    }

    [Fact]
    public void can_create_migrator()
    {
        var migrator = new SqliteMigrator();

        migrator.ShouldNotBeNull();
        migrator.Provider.ShouldBe(SqliteProvider.Instance);
    }

    [Fact]
    public async Task complete_workflow_example()
    {
        await using var dataSource = new SqliteDataSource("Data Source=:memory:");
        await using var connection = await dataSource.OpenConnectionAsync();

        // Create migrator
        var migrator = new SqliteMigrator();

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

    [Fact]
    public void detects_in_memory_connection_strings()
    {
        SqliteDataSource.IsInMemoryConnectionString("Data Source=:memory:").ShouldBeTrue();
        SqliteDataSource.IsInMemoryConnectionString("Data Source=test;Mode=Memory;Cache=Shared").ShouldBeTrue();
        SqliteDataSource.IsInMemoryConnectionString("Data Source=/tmp/test.db").ShouldBeFalse();
    }

    [Fact]
    public async Task keeps_in_memory_database_alive()
    {
        var connStr = $"Data Source=test_{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
        await using var dataSource = new SqliteDataSource(connStr);

        // Create a table on one connection
        await using (var conn1 = await dataSource.OpenConnectionAsync())
        {
            var cmd = conn1.CreateCommand();
            cmd.CommandText = "CREATE TABLE test (id INTEGER PRIMARY KEY)";
            await cmd.ExecuteNonQueryAsync();
        }
        // conn1 is now closed, but keep-alive should preserve the DB

        // Verify table still exists on a new connection
        await using var conn2 = await dataSource.OpenConnectionAsync();
        var checkCmd = conn2.CreateCommand();
        checkCmd.CommandText = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='test'";
        var count = (long)(await checkCmd.ExecuteScalarAsync())!;
        count.ShouldBe(1);
    }
}
