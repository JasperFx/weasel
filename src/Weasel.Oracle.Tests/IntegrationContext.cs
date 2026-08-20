using System.Data;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using Xunit;

namespace Weasel.Oracle.Tests;

[Collection("integration")]
public abstract class IntegrationContext: IAsyncLifetime
{
    private readonly string _schemaName;
    protected readonly OracleConnection theConnection = new OracleConnection(ConnectionSource.ConnectionString);

    protected IntegrationContext(string schemaName)
    {
        _schemaName = schemaName.ToUpperInvariant();
    }

    /// <summary>
    ///     Reset the schema under test. Safe to call more than once in a test: ODP.NET throws
    ///     ORA-50005 on a second <c>OpenAsync</c> against an open connection, which made a second
    ///     reset look like a product failure rather than a fixture one.
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
        var rules = new OracleMigrator();
        var writer = new StringWriter();
        schemaObject.WriteCreateStatement(rules, writer);

        var sql = writer.ToString();

        // Oracle can only execute one statement at a time
        // Split by "/" which is the Oracle statement separator
        var statements = sql.Split(new[] { "\n/\n", "\n/", "/\n" }, StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .ToArray();

        try
        {
            foreach (var statement in statements)
            {
                await theConnection.CreateCommand(statement)
                    .ExecuteNonQueryAsync();
            }
        }
        catch (Exception e)
        {
            throw new Exception("DDL Execution Failure.\n" + sql, e);
        }
    }

    protected async Task DropSchemaObjectInDatabase(ISchemaObject schemaObject)
    {
        var rules = new OracleMigrator();
        var writer = new StringWriter();
        schemaObject.WriteDropStatement(rules, writer);

        var sql = writer.ToString();

        // Oracle can only execute one statement at a time
        // Split by "/" which is the Oracle statement separator
        var statements = sql.Split(new[] { "\n/\n", "\n/", "/\n" }, StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .ToArray();

        foreach (var statement in statements)
        {
            await theConnection.CreateCommand(statement)
                .ExecuteNonQueryAsync();
        }
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
