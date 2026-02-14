using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Sqlite.Tests;

public class DatabaseWithTablesTests
{
    [Fact]
    public void migrator_creates_database()
    {
        var migrator = new SqliteMigrator();
        using var connection = new SqliteConnection("Data Source=:memory:");
        var db = migrator.CreateDatabase(connection);
        db.ShouldBeOfType<DatabaseWithTables>();
    }

    [Fact]
    public void create_table_returns_configurable_table()
    {
        var db = new DatabaseWithTables("test", "Data Source=:memory:");
        var table = db.AddTable(new DbObjectName("main", "dwt_people"));
        table.ShouldNotBeNull();
        db.Tables.Count.ShouldBe(1);
        db.Tables[0].ShouldBeSameAs(table);
    }

    [Fact]
    public async Task apply_migration_creates_tables()
    {
        var connectionString = $"Data Source={Path.GetTempFileName()};";

        var db = new DatabaseWithTables("test", connectionString);
        var table = db.AddTable(new DbObjectName("main", "dwt_users"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();
        await db.AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task detect_and_apply_schema_changes()
    {
        var connectionString = $"Data Source={Path.GetTempFileName()};";

        var db = new DatabaseWithTables("test", connectionString);
        var table = db.AddTable(new DbObjectName("main", "dwt_contacts"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();

        table.AddColumn("email", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();
        await db.AssertDatabaseMatchesConfigurationAsync();
    }
}
