using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests;

[Collection("integration")]
public class DatabaseWithTablesTests
{
    [Fact]
    public void migrator_creates_database()
    {
        var migrator = new SqlServerMigrator();
        using var connection = new SqlConnection(ConnectionSource.ConnectionString);
        var db = migrator.CreateDatabase(connection);
        db.ShouldBeOfType<DatabaseWithTables>();
    }

    [Fact]
    public void create_table_returns_configurable_table()
    {
        var db = new DatabaseWithTables("test", ConnectionSource.ConnectionString);
        var table = db.AddTable(new DbObjectName("dbo", "dwt_people"));
        table.ShouldNotBeNull();
        db.Tables.Count.ShouldBe(1);
        db.Tables[0].ShouldBeSameAs(table);
    }

    [Fact]
    public async Task apply_migration_creates_tables()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await conn.CreateCommand("IF OBJECT_ID('dbo.dwt_users', 'U') IS NOT NULL DROP TABLE dbo.dwt_users;")
            .ExecuteNonQueryAsync();

        var db = new DatabaseWithTables("test", ConnectionSource.ConnectionString);
        var table = db.AddTable(new DbObjectName("dbo", "dwt_users"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();
        await db.AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task detect_and_apply_schema_changes()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await conn.CreateCommand("IF OBJECT_ID('dbo.dwt_contacts', 'U') IS NOT NULL DROP TABLE dbo.dwt_contacts;")
            .ExecuteNonQueryAsync();

        var db = new DatabaseWithTables("test", ConnectionSource.ConnectionString);
        var table = db.AddTable(new DbObjectName("dbo", "dwt_contacts"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();

        table.AddColumn("email", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();
        await db.AssertDatabaseMatchesConfigurationAsync();
    }
}
