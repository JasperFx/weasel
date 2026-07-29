using JasperFx;
using JasperFx.Core.Reflection;
using Npgsql;
using Weasel.Core;
using Xunit;

namespace Weasel.Postgresql.Tests;

public abstract class IntegrationContext: IAsyncLifetime
{
    protected readonly NpgsqlDataSource theDataSource = NpgsqlDataSource.Create(ConnectionSource.ConnectionString);
    private NpgsqlConnection? connection;

    protected NpgsqlConnection theConnection
    {
        get => (connection ??= theDataSource.CreateConnection());
    }

    protected IntegrationContext(string schemaName)
    {
        if (!GetType().HasAttribute<CollectionAttribute>())
        {
            throw new InvalidOperationException(
                "You must decorate this class with a [Collection(\"schemaname\"] attribute. Preferably w/ the schema name");
        }

        SchemaName = schemaName;
    }

    public string SchemaName { get; }

    protected async Task ResetSchema()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync(SchemaName);
    }

    protected async Task CreateSchemaObjectInDatabase(ISchemaObject schemaObject)
    {
        var rules = new PostgresqlMigrator();
        var builder = new DbCommandBuilder(theConnection);
        schemaObject.ConfigureQueryCommand(builder);
        await using var reader = await theConnection.ExecuteReaderAsync(builder);
        var delta = await schemaObject.CreateDeltaAsync(reader);
        var schemaMigration = new SchemaMigration(delta);
        try
        {
            await reader.CloseAsync();
        }
        catch (NpgsqlException e)
        {
            // Quirk of postgres metadata tables, this will throw on the partition querying if the table does not already exist
            if (e.SqlState != PostgresErrorCodes.UndefinedTable)
            {
                throw;
            }
        }

        try
        {
            await rules.ApplyAllAsync(theConnection, schemaMigration, AutoCreate.All);
        }
        catch (Exception e)
        {
            throw new Exception("DDL Execution Failure.\n", e);
        }
    }

    protected Task DropSchemaObjectInDatabase(ISchemaObject schemaObject)
    {
        var rules = new PostgresqlMigrator();
        var writer = new StringWriter();
        schemaObject.WriteDropStatement(rules, writer);

        return theConnection.CreateCommand(writer.ToString())
            .ExecuteNonQueryAsync();
    }

    public virtual ValueTask InitializeAsync()
    {
        return ValueTask.CompletedTask;
    }

    // Connection teardown lives here, not in an IDisposable.Dispose. xUnit v3's
    // IAsyncLifetime inherits IAsyncDisposable, and when a test class implements both
    // IAsyncDisposable and IDisposable, v3 calls DisposeAsync only - so a Dispose()
    // holding the cleanup would silently never run and leak a connection per test.
    public virtual async ValueTask DisposeAsync()
    {
        if (connection != null)
        {
            await connection.DisposeAsync();
        }

        await theDataSource.DisposeAsync();
    }
}
