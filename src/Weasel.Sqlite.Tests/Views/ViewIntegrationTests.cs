using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Weasel.Sqlite.Views;
using Xunit;

namespace Weasel.Sqlite.Tests.Views;

public class ViewIntegrationTests
{
    private static async Task<SqliteConnection> OpenConnectionAsync()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        return connection;
    }

    private static async Task CreateUsersTable(SqliteConnection connection)
    {
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("email").NotNull();
        table.AddColumn<bool>("active");

        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        table.WriteCreateStatement(migrator, writer);

        var cmd = connection.CreateCommand();
        cmd.CommandText = writer.ToString();
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task CreateOrdersTable(SqliteConnection connection)
    {
        var table = new Table("orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("user_id");
        table.AddColumn<double>("amount");
        table.AddColumn<string>("created_at");

        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        table.WriteCreateStatement(migrator, writer);

        var cmd = connection.CreateCommand();
        cmd.CommandText = writer.ToString();
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task create_simple_view()
    {
        await using var connection = await OpenConnectionAsync();
        await CreateUsersTable(connection);

        var view = new View("active_users", "SELECT id, name, email FROM users WHERE active = 1");

        // Verify delta detects missing view before creation
        var queryCmd = connection.CreateCommand();
        var builder = new Core.DbCommandBuilder(queryCmd);
        view.ConfigureQueryCommand(builder);
        builder.Compile();

        await using var reader = await queryCmd.ExecuteReaderAsync();
        var delta = await view.CreateDeltaAsync(reader, CancellationToken.None);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        view.WriteCreateStatement(migrator, writer);

        var cmd = connection.CreateCommand();
        cmd.CommandText = writer.ToString();
        await cmd.ExecuteNonQueryAsync();

        // Verify view exists
        var exists = await view.ExistsInDatabaseAsync(connection);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task view_can_be_queried()
    {
        await using var connection = await OpenConnectionAsync();
        await CreateUsersTable(connection);

        // Insert test data
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = @"
            INSERT INTO users (id, name, email, active) VALUES
            (1, 'Alice', 'alice@example.com', 1),
            (2, 'Bob', 'bob@example.com', 0),
            (3, 'Charlie', 'charlie@example.com', 1)";
        await insertCmd.ExecuteNonQueryAsync();

        // Create view
        var view = new View("active_users", "SELECT id, name, email FROM users WHERE active = 1");
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        view.WriteCreateStatement(migrator, writer);

        var createCmd = connection.CreateCommand();
        createCmd.CommandText = writer.ToString();
        await createCmd.ExecuteNonQueryAsync();

        // Query the view
        var queryCmd = connection.CreateCommand();
        queryCmd.CommandText = "SELECT COUNT(*) FROM active_users";
        var count = (long)(await queryCmd.ExecuteScalarAsync())!;

        count.ShouldBe(2); // Only Alice and Charlie are active
    }

    [Fact]
    public async Task fetch_existing_view()
    {
        await using var connection = await OpenConnectionAsync();
        await CreateUsersTable(connection);

        var view = new View("active_users", "SELECT * FROM users WHERE active = 1");

        // Create the view
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        view.WriteCreateStatement(migrator, writer);

        var cmd = connection.CreateCommand();
        cmd.CommandText = writer.ToString();
        await cmd.ExecuteNonQueryAsync();

        // Fetch it back
        var fetched = await view.FetchExistingAsync(connection);

        fetched.ShouldNotBeNull();
        fetched!.Identifier.Name.ShouldBe("active_users");
        fetched.ViewSql.ShouldContain("SELECT * FROM users WHERE active = 1");

        // Verify delta shows no changes when view matches
        var queryCmd = connection.CreateCommand();
        var builder = new Core.DbCommandBuilder(queryCmd);
        view.ConfigureQueryCommand(builder);
        builder.Compile();

        await using var reader = await queryCmd.ExecuteReaderAsync();
        var delta = await view.CreateDeltaAsync(reader, CancellationToken.None);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task fetch_non_existent_view_returns_null()
    {
        await using var connection = await OpenConnectionAsync();

        var view = new View("non_existent", "SELECT 1");
        var fetched = await view.FetchExistingAsync(connection);

        fetched.ShouldBeNull();
    }

    [Fact]
    public async Task create_view_with_join()
    {
        await using var connection = await OpenConnectionAsync();
        await CreateUsersTable(connection);
        await CreateOrdersTable(connection);

        // Insert test data
        var insertUsersCmd = connection.CreateCommand();
        insertUsersCmd.CommandText = "INSERT INTO users (id, name, email, active) VALUES (1, 'Alice', 'alice@example.com', 1)";
        await insertUsersCmd.ExecuteNonQueryAsync();

        var insertOrdersCmd = connection.CreateCommand();
        insertOrdersCmd.CommandText = @"
            INSERT INTO orders (id, user_id, amount, created_at) VALUES
            (1, 1, 100.50, '2024-01-01'),
            (2, 1, 75.25, '2024-01-02')";
        await insertOrdersCmd.ExecuteNonQueryAsync();

        // Create view with join
        var viewSql = @"
            SELECT u.id, u.name, COUNT(o.id) as order_count, SUM(o.amount) as total_amount
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            GROUP BY u.id, u.name";

        var view = new View("user_order_summary", viewSql);

        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        view.WriteCreateStatement(migrator, writer);

        var createCmd = connection.CreateCommand();
        createCmd.CommandText = writer.ToString();
        await createCmd.ExecuteNonQueryAsync();

        // Query the view
        var queryCmd = connection.CreateCommand();
        queryCmd.CommandText = "SELECT order_count, total_amount FROM user_order_summary WHERE id = 1";
        await using var reader = await queryCmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        var orderCount = await reader.GetFieldValueAsync<long>(0);
        var totalAmount = await reader.GetFieldValueAsync<double>(1);

        orderCount.ShouldBe(2);
        totalAmount.ShouldBe(175.75);
    }

    [Fact]
    public async Task drop_existing_view()
    {
        await using var connection = await OpenConnectionAsync();
        await CreateUsersTable(connection);

        var view = new View("active_users", "SELECT * FROM users WHERE active = 1");

        // Create the view
        var createWriter = new StringWriter();
        var migrator = new SqliteMigrator();
        view.WriteCreateStatement(migrator, createWriter);

        var createCmd = connection.CreateCommand();
        createCmd.CommandText = createWriter.ToString();
        await createCmd.ExecuteNonQueryAsync();

        // Verify it exists
        var existsBefore = await view.ExistsInDatabaseAsync(connection);
        existsBefore.ShouldBeTrue();

        // Drop it
        var dropWriter = new StringWriter();
        view.WriteDropStatement(migrator, dropWriter);

        var dropCmd = connection.CreateCommand();
        dropCmd.CommandText = dropWriter.ToString();
        await dropCmd.ExecuteNonQueryAsync();

        // Verify it's gone
        var existsAfter = await view.ExistsInDatabaseAsync(connection);
        existsAfter.ShouldBeFalse();
    }

    [Fact]
    public async Task update_view_by_drop_and_recreate()
    {
        await using var connection = await OpenConnectionAsync();
        await CreateUsersTable(connection);

        // Create initial view
        var view1 = new View("active_users", "SELECT id, name FROM users WHERE active = 1");

        var createWriter = new StringWriter();
        var migrator = new SqliteMigrator();
        view1.WriteCreateStatement(migrator, createWriter);

        var createCmd = connection.CreateCommand();
        createCmd.CommandText = createWriter.ToString();
        await createCmd.ExecuteNonQueryAsync();

        // Verify delta detects the view change
        var view2 = new View("active_users", "SELECT id, name, email FROM users WHERE active = 1");

        var queryCmd = connection.CreateCommand();
        var builder = new Core.DbCommandBuilder(queryCmd);
        view2.ConfigureQueryCommand(builder);
        builder.Compile();

        await using var reader = await queryCmd.ExecuteReaderAsync();
        var delta = await view2.CreateDeltaAsync(reader, CancellationToken.None);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        // Apply the update
        var updateWriter = new StringWriter();
        view2.WriteCreateStatement(migrator, updateWriter);

        var updateCmd = connection.CreateCommand();
        updateCmd.CommandText = updateWriter.ToString();
        await updateCmd.ExecuteNonQueryAsync();

        // Verify the updated view exists and has the new definition
        var fetched = await view2.FetchExistingAsync(connection);
        fetched.ShouldNotBeNull();
        fetched!.ViewSql.ShouldContain("email");
    }

    [Fact]
    public async Task view_with_json_extract()
    {
        await using var connection = await OpenConnectionAsync();

        // Create table with JSON column
        var table = new Table("products");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.AddColumn<string>("metadata"); // JSON column

        var tableWriter = new StringWriter();
        var migrator = new SqliteMigrator();
        table.WriteCreateStatement(migrator, tableWriter);

        var createTableCmd = connection.CreateCommand();
        createTableCmd.CommandText = tableWriter.ToString();
        await createTableCmd.ExecuteNonQueryAsync();

        // Insert test data
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = @"
            INSERT INTO products (id, name, metadata) VALUES
            (1, 'Widget', '{""category"": ""gadgets"", ""price"": 19.99}'),
            (2, 'Gizmo', '{""category"": ""tools"", ""price"": 29.99}')";
        await insertCmd.ExecuteNonQueryAsync();

        // Create view that extracts JSON fields
        var viewSql = @"
            SELECT
                id,
                name,
                json_extract(metadata, '$.category') as category,
                json_extract(metadata, '$.price') as price
            FROM products";

        var view = new View("product_details", viewSql);

        var viewWriter = new StringWriter();
        view.WriteCreateStatement(migrator, viewWriter);

        var createViewCmd = connection.CreateCommand();
        createViewCmd.CommandText = viewWriter.ToString();
        await createViewCmd.ExecuteNonQueryAsync();

        // Query the view
        var queryCmd = connection.CreateCommand();
        queryCmd.CommandText = "SELECT category, price FROM product_details WHERE id = 1";
        await using var reader = await queryCmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        var category = await reader.GetFieldValueAsync<string>(0);
        var price = await reader.GetFieldValueAsync<double>(1);

        category.ShouldBe("gadgets");
        price.ShouldBe(19.99);
    }
}
