using JasperFx.Core.Reflection;
using Npgsql;
using Weasel.Core;
using Xunit;

namespace Weasel.Postgresql.Tests;

public abstract class IntegrationContext: IDisposable, IAsyncLifetime
{
    protected readonly NpgsqlConnection theConnection = new NpgsqlConnection(ConnectionSource.ConnectionString);

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

    public void Dispose()
    {
        theConnection?.Dispose();
    }

    protected async Task ResetSchema()
    {
        await theConnection.OpenAsync();

        await theConnection.ResetSchemaAsync(SchemaName);
    }

    protected async Task CreateSchemaObjectInDatabase(ISchemaObject schemaObject)
    {
        var rules = new PostgresqlMigrator();
        var writer = new StringWriter();
        schemaObject.WriteCreateStatement(rules, writer);

        try
        {
            await theConnection.CreateCommand(writer.ToString())
                .ExecuteNonQueryAsync();
        }
        catch (Exception e)
        {
            throw new Exception("DDL Execution Failure.\n" + writer, e);
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

    public virtual Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    public virtual Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
}
