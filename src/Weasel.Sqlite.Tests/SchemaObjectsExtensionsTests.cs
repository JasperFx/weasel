using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Weasel.Sqlite.Views;
using Xunit;

namespace Weasel.Sqlite.Tests;

[Collection("integration")]
public class SchemaObjectsExtensionsTests
{
    private static async Task<SqliteConnection> OpenConnectionAsync()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        return connection;
    }

    [Fact]
    public async Task apply_changes_creates_new_table()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email");

        await table.ApplyChangesAsync(connection);

        var tables = await connection.ExistingTablesAsync();
        tables.ShouldContain(x => x.Name == "users");
    }

    [Fact]
    public async Task drop_async_removes_table()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");

        await table.CreateAsync(connection);

        var tables = await connection.ExistingTablesAsync();
        tables.ShouldContain(x => x.Name == "users");

        await table.DropAsync(connection);

        var tablesAfterDrop = await connection.ExistingTablesAsync();
        tablesAfterDrop.ShouldNotContain(x => x.Name == "users");
    }

    [Fact]
    public async Task create_async_creates_schema_object()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("products");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<decimal>("price");

        await table.CreateAsync(connection);

        var tables = await connection.ExistingTablesAsync();
        tables.ShouldContain(x => x.Name == "products");
    }

    [Fact]
    public async Task ensure_schema_exists_is_noop_for_sqlite()
    {
        await using var connection = await OpenConnectionAsync();

        // Should not throw
        await connection.EnsureSchemaExists("main");
        await connection.EnsureSchemaExists("some_schema");
    }

    [Fact]
    public async Task active_schema_names_returns_main()
    {
        await using var connection = await OpenConnectionAsync();

        var schemas = await connection.ActiveSchemaNamesAsync();

        schemas.ShouldContain("main");
        // Note: "temp" schema may or may not be present depending on SQLite state
    }

    [Fact]
    public async Task drop_schema_async_removes_all_tables_and_views()
    {
        await using var connection = await OpenConnectionAsync();

        // Create tables
        var table1 = new Table("users");
        table1.AddColumn<int>("id").AsPrimaryKey();
        await table1.CreateAsync(connection);

        var table2 = new Table("orders");
        table2.AddColumn<int>("id").AsPrimaryKey();
        await table2.CreateAsync(connection);

        // Create a view
        var view = new View("active_users", "SELECT id FROM users");
        await view.CreateAsync(connection);

        // Create an index
        await connection.CreateCommand("CREATE INDEX idx_users_id ON users(id)").ExecuteNonQueryAsync();

        // Verify objects exist
        (await connection.ExistingTablesAsync()).Count.ShouldBeGreaterThanOrEqualTo(2);
        (await connection.ExistingViewsAsync()).Count.ShouldBeGreaterThanOrEqualTo(1);

        // Drop schema
        await connection.DropSchemaAsync("main");

        // Verify objects are gone
        (await connection.ExistingTablesAsync()).ShouldBeEmpty();
        (await connection.ExistingViewsAsync()).ShouldBeEmpty();
    }

    [Fact]
    public async Task create_schema_async_is_noop_for_sqlite()
    {
        await using var connection = await OpenConnectionAsync();

        // Should not throw
        await connection.CreateSchemaAsync("main");
        await connection.CreateSchemaAsync("some_schema");
    }

    [Fact]
    public async Task reset_schema_async_clears_all_objects()
    {
        await using var connection = await OpenConnectionAsync();

        // Create a table
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        await table.CreateAsync(connection);

        (await connection.ExistingTablesAsync()).ShouldNotBeEmpty();

        // Reset schema
        await connection.ResetSchemaAsync("main");

        // Verify table is gone
        (await connection.ExistingTablesAsync()).ShouldBeEmpty();
    }

    [Fact]
    public async Task function_exists_always_returns_false()
    {
        await using var connection = await OpenConnectionAsync();

        var functionName = new SqliteObjectName("my_function");
        var exists = await connection.FunctionExistsAsync(functionName);

        exists.ShouldBeFalse();
    }

    [Fact]
    public async Task existing_tables_async_returns_all_tables()
    {
        await using var connection = await OpenConnectionAsync();

        var table1 = new Table("users");
        table1.AddColumn<int>("id").AsPrimaryKey();
        await table1.CreateAsync(connection);

        var table2 = new Table("orders");
        table2.AddColumn<int>("id").AsPrimaryKey();
        await table2.CreateAsync(connection);

        var table3 = new Table("products");
        table3.AddColumn<int>("id").AsPrimaryKey();
        await table3.CreateAsync(connection);

        var tables = await connection.ExistingTablesAsync();

        tables.ShouldContain(x => x.Name == "users");
        tables.ShouldContain(x => x.Name == "orders");
        tables.ShouldContain(x => x.Name == "products");
        tables.Count.ShouldBe(3);
    }

    [Fact]
    public async Task existing_tables_async_filters_by_name_pattern()
    {
        await using var connection = await OpenConnectionAsync();

        var table1 = new Table("mt_users");
        table1.AddColumn<int>("id").AsPrimaryKey();
        await table1.CreateAsync(connection);

        var table2 = new Table("orders");
        table2.AddColumn<int>("id").AsPrimaryKey();
        await table2.CreateAsync(connection);

        var table3 = new Table("mt_products");
        table3.AddColumn<int>("id").AsPrimaryKey();
        await table3.CreateAsync(connection);

        var tables = await connection.ExistingTablesAsync("mt_%");

        tables.ShouldContain(x => x.Name == "mt_users");
        tables.ShouldContain(x => x.Name == "mt_products");
        tables.ShouldNotContain(x => x.Name == "orders");
        tables.Count.ShouldBe(2);
    }

    [Fact]
    public async Task existing_tables_async_filters_by_schema()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        await table.CreateAsync(connection);

        var tables = await connection.ExistingTablesAsync(schemas: new[] { "main" });

        tables.ShouldContain(x => x.Name == "users");
        tables.Count.ShouldBe(1);
    }

    [Fact]
    public async Task existing_views_async_returns_all_views()
    {
        await using var connection = await OpenConnectionAsync();

        // Create table first
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.AddColumn<bool>("active");
        await table.CreateAsync(connection);

        // Create views
        var view1 = new View("active_users", "SELECT id, name FROM users WHERE active = 1");
        await view1.CreateAsync(connection);

        var view2 = new View("inactive_users", "SELECT id, name FROM users WHERE active = 0");
        await view2.CreateAsync(connection);

        var views = await connection.ExistingViewsAsync();

        views.ShouldContain(x => x.Name == "active_users");
        views.ShouldContain(x => x.Name == "inactive_users");
        views.Count.ShouldBe(2);
    }

    [Fact]
    public async Task existing_views_async_filters_by_name_pattern()
    {
        await using var connection = await OpenConnectionAsync();

        // Create table first
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        await table.CreateAsync(connection);

        // Create views
        var view1 = new View("vw_active_users", "SELECT * FROM users");
        await view1.CreateAsync(connection);

        var view2 = new View("summary_report", "SELECT * FROM users");
        await view2.CreateAsync(connection);

        var views = await connection.ExistingViewsAsync("vw_%");

        views.ShouldContain(x => x.Name == "vw_active_users");
        views.ShouldNotContain(x => x.Name == "summary_report");
        views.Count.ShouldBe(1);
    }

    [Fact]
    public async Task existing_functions_always_returns_empty()
    {
        await using var connection = await OpenConnectionAsync();

        var functions = await connection.ExistingFunctionsAsync();

        functions.ShouldBeEmpty();
    }

    [Fact]
    public async Task to_create_sql_generates_ddl()
    {
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email");

        var migrator = new SqliteMigrator();
        var sql = table.ToCreateSql(migrator);

        sql.ShouldNotBeNullOrEmpty();
        sql.ShouldContain("CREATE TABLE");
        sql.ShouldContain("users");
        sql.ShouldContain("id");
        sql.ShouldContain("name");
        sql.ShouldContain("email");
    }

    [Fact]
    public async Task migrate_async_creates_new_table()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();

        var migrated = await table.MigrateAsync(connection);

        migrated.ShouldBeTrue();

        var tables = await connection.ExistingTablesAsync();
        tables.ShouldContain(x => x.Name == "users");
    }

    [Fact]
    public async Task migrate_async_creates_table()
    {
        await using var connection = await OpenConnectionAsync();

        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");

        // First migration
        var migrated1 = await table.MigrateAsync(connection);
        migrated1.ShouldBeTrue();

        // Verify table exists
        var tables = await connection.ExistingTablesAsync();
        tables.ShouldContain(x => x.Name == "users");

        // Note: Delta detection for tables is not yet fully implemented (Table.Deltas.cs)
        // so we cannot test "no changes" scenario yet
    }

    [Fact]
    public async Task migrate_async_with_array_of_schema_objects()
    {
        await using var connection = await OpenConnectionAsync();

        var table1 = new Table("users");
        table1.AddColumn<int>("id").AsPrimaryKey();
        table1.AddColumn<string>("name");

        var table2 = new Table("orders");
        table2.AddColumn<int>("id").AsPrimaryKey();
        table2.AddColumn<int>("user_id");

        ISchemaObject[] objects = { table1, table2 };

        var migrated = await objects.MigrateAsync(connection);

        migrated.ShouldBeTrue();

        var tables = await connection.ExistingTablesAsync();
        tables.ShouldContain(x => x.Name == "users");
        tables.ShouldContain(x => x.Name == "orders");
    }

    [Fact]
    public async Task migrate_async_array_creates_multiple_tables()
    {
        await using var connection = await OpenConnectionAsync();

        var table1 = new Table("customers");
        table1.AddColumn<int>("id").AsPrimaryKey();

        var table2 = new Table("invoices");
        table2.AddColumn<int>("id").AsPrimaryKey();

        ISchemaObject[] objects = { table1, table2 };

        // First migration
        var migrated1 = await objects.MigrateAsync(connection);
        migrated1.ShouldBeTrue();

        // Verify tables exist
        var tables = await connection.ExistingTablesAsync();
        tables.ShouldContain(x => x.Name == "customers");
        tables.ShouldContain(x => x.Name == "invoices");

        // Note: Delta detection for tables is not yet fully implemented (Table.Deltas.cs)
        // so we cannot test "no changes" scenario yet
    }

    [Fact]
    public async Task migrate_async_opens_connection_if_closed()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        // Don't open the connection

        connection.State.ShouldBe(System.Data.ConnectionState.Closed);

        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();

        var migrated = await table.MigrateAsync(connection);

        migrated.ShouldBeTrue();
        connection.State.ShouldBe(System.Data.ConnectionState.Open);

        await connection.DisposeAsync();
    }

    [Fact]
    public async Task drop_schema_async_handles_triggers()
    {
        await using var connection = await OpenConnectionAsync();

        // Create a table
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        await table.CreateAsync(connection);

        // Create a trigger
        await connection.CreateCommand(@"
            CREATE TRIGGER update_timestamp
            AFTER UPDATE ON users
            BEGIN
                SELECT 1;
            END
        ").ExecuteNonQueryAsync();

        // Verify trigger exists
        var triggers = await connection.CreateCommand(
            "SELECT name FROM main.sqlite_master WHERE type = 'trigger'"
        ).FetchListAsync<string>();
        triggers.ShouldContain("update_timestamp");

        // Drop schema
        await connection.DropSchemaAsync("main");

        // Verify trigger is gone
        var triggersAfter = await connection.CreateCommand(
            "SELECT name FROM main.sqlite_master WHERE type = 'trigger'"
        ).FetchListAsync<string>();
        triggersAfter.ShouldBeEmpty();
    }

    [Fact]
    public async Task drop_schema_async_handles_indexes()
    {
        await using var connection = await OpenConnectionAsync();

        // Create a table
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("email");
        await table.CreateAsync(connection);

        // Create an index
        await connection.CreateCommand("CREATE INDEX idx_users_email ON users(email)").ExecuteNonQueryAsync();

        // Verify index exists
        var indexes = await connection.CreateCommand(
            "SELECT name FROM main.sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%'"
        ).FetchListAsync<string>();
        indexes.ShouldContain("idx_users_email");

        // Drop schema
        await connection.DropSchemaAsync("main");

        // Verify index is gone
        var indexesAfter = await connection.CreateCommand(
            "SELECT name FROM main.sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%'"
        ).FetchListAsync<string>();
        indexesAfter.ShouldBeEmpty();
    }
}
