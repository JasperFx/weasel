using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using Xunit;

namespace Weasel.Oracle.Tests;

[Collection("integration")]
public abstract class IntegrationContext: IDisposable, IAsyncLifetime
{
    private readonly string _schemaName;
    protected readonly OracleConnection theConnection = new OracleConnection(ConnectionSource.ConnectionString);

    protected IntegrationContext(string schemaName)
    {
        _schemaName = schemaName.ToUpperInvariant();
    }

    public void Dispose()
    {
        theConnection?.Dispose();
    }

    protected async Task ResetSchema()
    {
        await theConnection.OpenAsync();

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
        var statements = sql.Split(new[] { "\n/\n", "\n/" }, StringSplitOptions.RemoveEmptyEntries)
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
        var statements = sql.Split(new[] { "\n/\n", "\n/" }, StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .ToArray();

        foreach (var statement in statements)
        {
            await theConnection.CreateCommand(statement)
                .ExecuteNonQueryAsync();
        }
    }

    public virtual Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    public virtual Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
}
