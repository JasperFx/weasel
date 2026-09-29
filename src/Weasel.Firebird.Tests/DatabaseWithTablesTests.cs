using FirebirdSql.Data.FirebirdClient;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests;

public class DatabaseWithTablesTests: IntegrationContext
{
    [Fact]
    public void migrator_creates_database()
    {
        new FirebirdMigrator().CreateDatabase(new FbConnection(ConnectionString)).ShouldBeOfType<DatabaseWithTables>();
    }

    [Fact]
    public void create_table_returns_configurable_table()
    {
        var db = new DatabaseWithTables("test", ConnectionString);
        var table = db.AddTable(new FirebirdObjectName("dwt_people"));

        table.ShouldBeOfType<Table>();
        db.Tables.Single().ShouldBeSameAs(table);
    }

    [Fact]
    public async Task apply_migration_creates_tables()
    {
        var db = new DatabaseWithTables("test", ConnectionString);
        var table = db.AddTable(new FirebirdObjectName("dwt_users"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));

        (await db.ApplyAllConfiguredChangesToDatabaseAsync()).ShouldBe(SchemaPatchDifference.Create);
        await db.AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task detect_and_apply_schema_changes()
    {
        var db = new DatabaseWithTables("test", ConnectionString);
        var table = db.AddTable(new FirebirdObjectName("dwt_contacts"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();

        table.AddColumn("email", typeof(string));
        table.AddIndex("idx_dwt_contacts_email", ["email"], isUnique: true);

        (await db.ApplyAllConfiguredChangesToDatabaseAsync()).ShouldBe(SchemaPatchDifference.Update);
        await db.AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task an_unchanged_database_applies_nothing()
    {
        var db = new DatabaseWithTables("test", ConnectionString);
        var table = db.AddTable(new FirebirdObjectName("dwt_things"));
        table.AddPrimaryKeyColumn("id", typeof(int));

        await db.ApplyAllConfiguredChangesToDatabaseAsync();

        (await db.ApplyAllConfiguredChangesToDatabaseAsync()).ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_table_the_database_does_not_match_is_reported()
    {
        await ExecuteAsync("CREATE TABLE dwt_drifted (id INTEGER NOT NULL, CONSTRAINT pk_dwt_drifted PRIMARY KEY (id))");

        var db = new DatabaseWithTables("test", ConnectionString);
        var table = db.AddTable(new FirebirdObjectName("dwt_drifted"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("added", typeof(int));

        await Should.ThrowAsync<Exception>(() => db.AssertDatabaseMatchesConfigurationAsync());
    }

    [Fact]
    public void describes_itself_as_firebird()
    {
        var descriptor = new DatabaseWithTables("test", ConnectionString).Describe();

        descriptor.Engine.ShouldBe("Firebird");
        descriptor.DatabaseName.ShouldEndWith("databasewithtablestests.fdb");
        descriptor.ServerName.ShouldBe(new FbConnectionStringBuilder(ConnectionString).DataSource);
    }
}
