using Oracle.ManagedDataAccess.Client;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Oracle.Tests;

[Collection("integration")]
public class DatabaseWithTablesTests
{
    [Fact]
    public void migrator_creates_database()
    {
        var migrator = new OracleMigrator();
        using var connection = new OracleConnection(ConnectionSource.ConnectionString);
        var db = migrator.CreateDatabase(connection);
        db.ShouldBeOfType<DatabaseWithTables>();
    }

    [Fact]
    public void create_table_returns_configurable_table()
    {
        var db = new DatabaseWithTables("test", ConnectionSource.ConnectionString);
        var table = db.AddTable(new DbObjectName("WEASEL", "DWT_PEOPLE"));
        table.ShouldNotBeNull();
        db.Tables.Count.ShouldBe(1);
        db.Tables[0].ShouldBeSameAs(table);
    }

    [Fact]
    public async Task apply_migration_creates_tables()
    {
        await using var conn = new OracleConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        try
        {
            await conn.CreateCommand("DROP TABLE WEASEL.DWT_USERS").ExecuteNonQueryAsync();
        }
        catch
        {
            // Table may not exist
        }

        var db = new DatabaseWithTables("test", ConnectionSource.ConnectionString);
        var table = db.AddTable(new DbObjectName("WEASEL", "DWT_USERS"));
        table.AddPrimaryKeyColumn("ID", typeof(int));
        table.AddColumn("NAME", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();
        await db.AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task detect_and_apply_schema_changes()
    {
        await using var conn = new OracleConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        try
        {
            await conn.CreateCommand("DROP TABLE WEASEL.DWT_CONTACTS").ExecuteNonQueryAsync();
        }
        catch
        {
            // Table may not exist
        }

        var db = new DatabaseWithTables("test", ConnectionSource.ConnectionString);
        var table = db.AddTable(new DbObjectName("WEASEL", "DWT_CONTACTS"));
        table.AddPrimaryKeyColumn("ID", typeof(int));
        table.AddColumn("NAME", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();

        table.AddColumn("EMAIL", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();
        await db.AssertDatabaseMatchesConfigurationAsync();
    }
}
