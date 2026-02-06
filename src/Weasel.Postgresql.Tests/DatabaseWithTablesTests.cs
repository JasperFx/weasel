using Npgsql;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Postgresql.Tests;

[Collection("integration")]
public class DatabaseWithTablesTests: IntegrationContext
{
    public DatabaseWithTablesTests(): base("integration")
    {
    }

    [Fact]
    public void migrator_creates_database_from_data_source()
    {
        var migrator = new PostgresqlMigrator();
        var db = migrator.CreateDatabase(theDataSource);
        db.ShouldBeOfType<DatabaseWithTables>();
    }

    [Fact]
    public void migrator_creates_database_from_connection()
    {
        var migrator = new PostgresqlMigrator();
        using var connection = new NpgsqlConnection(ConnectionSource.ConnectionString);
        var db = migrator.CreateDatabase(connection);
        db.ShouldBeOfType<DatabaseWithTables>();
    }

    [Fact]
    public void create_table_returns_configurable_table()
    {
        var db = new DatabaseWithTables("test", theDataSource);
        var table = db.CreateTable(new PostgresqlObjectName("public", "dwt_people"));
        table.ShouldNotBeNull();
        db.Tables.Count.ShouldBe(1);
        db.Tables[0].ShouldBeSameAs(table);
    }

    [Fact]
    public async Task apply_migration_creates_tables()
    {
        await ResetSchema();

        var db = new DatabaseWithTables("test", theDataSource);
        var table = db.CreateTable(new PostgresqlObjectName("public", "dwt_users"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();
        await db.AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task detect_and_apply_schema_changes()
    {
        await ResetSchema();

        var db = new DatabaseWithTables("test", theDataSource);
        var table = db.CreateTable(new PostgresqlObjectName("public", "dwt_contacts"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();

        table.AddColumn("email", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();
        await db.AssertDatabaseMatchesConfigurationAsync();
    }
}
