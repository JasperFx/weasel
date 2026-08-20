using System.Data;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests;

[Collection("integration")]
public abstract class IntegrationContext: IAsyncLifetime
{
    private readonly string _schemaName;
    protected readonly SqlConnection theConnection = new SqlConnection(ConnectionSource.ConnectionString);

    protected IntegrationContext(string schemaName)
    {
        _schemaName = schemaName;
    }

    /// <summary>
    ///     Reset the schema under test. Safe to call more than once in a test: SqlClient throws
    ///     "The connection was not closed" on a second OpenAsync, which made a second reset look
    ///     like a product failure rather than a fixture one.
    /// </summary>
    protected async Task ResetSchema()
    {
        if (theConnection.State == ConnectionState.Closed)
        {
            await theConnection.OpenAsync();
        }

        await theConnection.ResetSchemaAsync(_schemaName);
    }

    protected async Task CreateSchemaObjectInDatabase(ISchemaObject schemaObject)
    {
        var rules = new SqlServerMigrator();
        var writer = new StringWriter();
        schemaObject.WriteCreateStatement(rules, writer);

        try
        {
            await theConnection.CreateCommand(writer.ToString())
                .ExecuteNonQueryAsync();
        }
        catch (Exception e)
        {
            throw new Exception("DDL Execution Failure.\n" + writer.ToString(), e);
        }
    }

    protected Task DropSchemaObjectInDatabase(ISchemaObject schemaObject)
    {
        var rules = new SqlServerMigrator();
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
        await theConnection.DisposeAsync();
    }
}
