using MySqlConnector;
using Weasel.Core;
using Xunit;

namespace Weasel.MySql.Tests;

[Collection("integration")]
public abstract class IntegrationContext: IAsyncLifetime
{
    protected MySqlConnection theConnection = default!;

    public async ValueTask InitializeAsync()
    {
        theConnection = await ConnectionSource.CreateOpenConnectionAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await theConnection.CloseAsync();
        await theConnection.DisposeAsync();
    }

    protected async Task ResetSchemaAsync(string schemaName)
    {
        await DropSchemaAsync(schemaName);
        await CreateSchemaAsync(schemaName);
    }

    // These were hand-rolled here because Weasel.MySql had no schema extensions at all -- the one
    // provider of five without them. It does now (weasel#465), so the fixture uses the shipped
    // API and the tests exercise it on every run.
    protected Task CreateSchemaAsync(string schemaName)
        => theConnection.CreateSchemaAsync(schemaName);

    protected Task DropSchemaAsync(string schemaName)
        => theConnection.DropSchemaAsync(schemaName);

    protected async Task CreateTableAsync(string sql)
    {
        await using var cmd = theConnection.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    protected async Task DropTableAsync(string tableName)
    {
        await using var cmd = theConnection.CreateCommand($"DROP TABLE IF EXISTS {tableName}");
        await cmd.ExecuteNonQueryAsync();
    }

    protected async Task<T> ExecuteScalarAsync<T>(string sql)
    {
        await using var cmd = theConnection.CreateCommand(sql);
        var result = await cmd.ExecuteScalarAsync();
        return (T)Convert.ChangeType(result!, typeof(T));
    }
}

[CollectionDefinition("integration")]
public class IntegrationCollection: ICollectionFixture<IntegrationFixture>
{
}

public class IntegrationFixture: IAsyncLifetime
{
    public async ValueTask InitializeAsync()
    {
        // Ensure database exists
        var builder = new MySqlConnectionStringBuilder(ConnectionSource.ConnectionString);
        var database = builder.Database;
        builder.Database = "";

        await using var conn = new MySqlConnection(builder.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"CREATE DATABASE IF NOT EXISTS `{database}`";
        await cmd.ExecuteNonQueryAsync();
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
