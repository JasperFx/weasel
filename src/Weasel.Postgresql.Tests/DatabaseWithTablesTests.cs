using Npgsql;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <remarks>
/// Every table here lives in the "integration" schema, which is what <see cref="IntegrationContext.ResetSchema" />
/// actually resets for this class. They used to be created in "public", which nothing here
/// isolates and which at least nine other test files also write to. xUnit serialises this
/// collection against itself but runs other collections in parallel, so a concurrent drop in
/// "public" would surface as either "relation ... does not exist" or, when it landed mid-scan
/// of pg_index, "could not open relation with OID". See weasel#407.
/// </remarks>
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
        var table = db.AddTable(new PostgresqlObjectName("integration", "dwt_people"));
        table.ShouldNotBeNull();
        db.Tables.Count.ShouldBe(1);
        db.Tables[0].ShouldBeSameAs(table);
    }

    [Fact]
    public async Task apply_migration_creates_tables()
    {
        await ResetSchema();

        var db = new DatabaseWithTables("test", theDataSource);
        var table = db.AddTable(new PostgresqlObjectName("integration", "dwt_users"));
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
        var table = db.AddTable(new PostgresqlObjectName("integration", "dwt_contacts"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();

        table.AddColumn("email", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();
        await db.AssertDatabaseMatchesConfigurationAsync();
    }
}
