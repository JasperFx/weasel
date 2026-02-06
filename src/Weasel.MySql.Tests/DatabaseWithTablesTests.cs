using MySqlConnector;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.MySql.Tests;

[Collection("integration")]
public class DatabaseWithTablesTests
{
    [Fact]
    public void migrator_creates_database()
    {
        var migrator = new MySqlMigrator();
        using var connection = new MySqlConnection(ConnectionSource.ConnectionString);
        var db = migrator.CreateDatabase(connection);
        db.ShouldBeOfType<DatabaseWithTables>();
    }

    [Fact]
    public void create_table_returns_configurable_table()
    {
        var db = new DatabaseWithTables("test", ConnectionSource.ConnectionString);
        var table = db.AddTable(new DbObjectName("weasel_testing", "dwt_people"));
        table.ShouldNotBeNull();
        db.Tables.Count.ShouldBe(1);
        db.Tables[0].ShouldBeSameAs(table);
    }

    [Fact]
    public async Task apply_migration_creates_tables()
    {
        await using var conn = new MySqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await conn.CreateCommand("DROP TABLE IF EXISTS `dwt_users`;")
            .ExecuteNonQueryAsync();

        var db = new DatabaseWithTables("test", ConnectionSource.ConnectionString);
        var table = db.AddTable(new DbObjectName("weasel_testing", "dwt_users"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();
        await db.AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task detect_and_apply_schema_changes()
    {
        await using var conn = new MySqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await conn.CreateCommand("DROP TABLE IF EXISTS `dwt_contacts`;")
            .ExecuteNonQueryAsync();

        var db = new DatabaseWithTables("test", ConnectionSource.ConnectionString);
        var table = db.AddTable(new DbObjectName("weasel_testing", "dwt_contacts"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();

        table.AddColumn("email", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();
        await db.AssertDatabaseMatchesConfigurationAsync();
    }
}
