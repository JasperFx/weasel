using MySqlConnector;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.MySql.Tests;

/// <summary>
///     weasel#465 audited schema teardown across the five providers and found MySQL was the only
///     one with no schema extensions at all: the drop lived inside
///     <c>MySqlMigrator.WriteSchemaDropSql</c> and as a private helper in the test fixture, and
///     nothing was callable from outside.
/// </summary>
/// <remarks>
///     <para>
///         MySQL is one of the two providers that cannot fall behind — a schema is a database and
///         <c>DROP DATABASE</c> cascades on the server, so no new creatable object type can survive
///         it. That is what these assert: not that a particular list of object types is handled,
///         but that nothing is left whatever the schema held.
///     </para>
///     <para>
///         Root credentials, following <c>MySqlMigratorTests.ensure_database_creates_database</c>:
///         the <c>weasel</c> user the other tests connect as is granted rights on
///         <c>weasel_testing</c> only, so it cannot create a database to tear down.
///     </para>
/// </remarks>
[Collection("integration")]
public class dropping_schemas: IAsyncLifetime
{
    private const string SchemaName = "weasel_teardown";

    private MySqlConnection theConnection = default!;

    public async ValueTask InitializeAsync()
    {
        var builder = new MySqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            UserID = "root", Password = "P@55w0rd", Database = ""
        };

        theConnection = new MySqlConnection(builder.ConnectionString);
        await theConnection.OpenAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await theConnection.DropSchemaAsync(SchemaName);
        await theConnection.CloseAsync();
        await theConnection.DisposeAsync();
    }

    private Task executeAsync(string sql)
        => theConnection.CreateCommand(sql).ExecuteNonQueryAsync();

    private async Task<int> tableCountAsync()
    {
        var count = await theConnection
            .CreateCommand("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @schema")
            .With("schema", SchemaName)
            .ExecuteScalarAsync();

        return Convert.ToInt32(count);
    }

    private async Task<bool> schemaExistsAsync()
    {
        var count = await theConnection
            .CreateCommand("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = @schema")
            .With("schema", SchemaName)
            .ExecuteScalarAsync();

        return Convert.ToInt32(count) > 0;
    }

    [Fact]
    public async Task create_then_drop_removes_the_schema_itself()
    {
        await theConnection.DropSchemaAsync(SchemaName);
        (await schemaExistsAsync()).ShouldBeFalse();

        await theConnection.CreateSchemaAsync(SchemaName);
        (await schemaExistsAsync()).ShouldBeTrue();

        await theConnection.DropSchemaAsync(SchemaName);
        (await schemaExistsAsync()).ShouldBeFalse();
    }

    [Fact]
    public async Task dropping_a_schema_that_is_not_there_is_not_an_error()
    {
        await theConnection.DropSchemaAsync(SchemaName);
        await theConnection.DropSchemaAsync(SchemaName);

        (await schemaExistsAsync()).ShouldBeFalse();
    }

    /// <summary>
    ///     A table, a view and a trigger — the shapes that broke the enumerating teardowns on SQL
    ///     Server (#464) and Oracle (#465). MySQL takes all of them with one statement.
    /// </summary>
    [Fact]
    public async Task nothing_in_the_schema_survives_the_drop()
    {
        await theConnection.ResetSchemaAsync(SchemaName);

        await executeAsync($"CREATE TABLE `{SchemaName}`.teardown_src (id INT PRIMARY KEY, qty INT)");
        await executeAsync(
            $"CREATE VIEW `{SchemaName}`.teardown_view AS SELECT id FROM `{SchemaName}`.teardown_src");
        await executeAsync(
            $"CREATE TRIGGER `{SchemaName}`.teardown_trg BEFORE INSERT ON `{SchemaName}`.teardown_src "
            + "FOR EACH ROW SET NEW.qty = NEW.qty");

        // information_schema.tables counts the view alongside the base table -- the very conflation
        // that hid SQL Server's teardown bug until weasel#464.
        (await tableCountAsync()).ShouldBe(2);

        await theConnection.DropSchemaAsync(SchemaName);

        (await schemaExistsAsync()).ShouldBeFalse();
        (await tableCountAsync()).ShouldBe(0);
    }

    [Fact]
    public async Task reset_leaves_an_empty_schema_behind()
    {
        await theConnection.ResetSchemaAsync(SchemaName);
        await executeAsync($"CREATE TABLE `{SchemaName}`.reset_src (id INT PRIMARY KEY)");

        (await tableCountAsync()).ShouldBe(1);

        await theConnection.ResetSchemaAsync(SchemaName);

        (await schemaExistsAsync()).ShouldBeTrue();
        (await tableCountAsync()).ShouldBe(0);
    }
}
