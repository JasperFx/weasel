using MySqlConnector;
using Weasel.Core;
using Xunit;

namespace Weasel.MySql.Tests;

[Collection("integration")]
public abstract class IntegrationContext: IAsyncLifetime
{
    protected MySqlConnection theConnection = default!;

    public async Task InitializeAsync()
    {
        theConnection = await ConnectionSource.CreateOpenConnectionAsync();
    }

    public async Task DisposeAsync()
    {
        await theConnection.CloseAsync();
        await theConnection.DisposeAsync();
    }

    protected async Task ResetSchemaAsync(string schemaName)
    {
        await DropSchemaAsync(schemaName);
        await CreateSchemaAsync(schemaName);
    }

    protected async Task CreateSchemaAsync(string schemaName)
    {
        await using var cmd = theConnection.CreateCommand($"CREATE DATABASE IF NOT EXISTS `{schemaName}`");
        await cmd.ExecuteNonQueryAsync();
    }

    protected async Task DropSchemaAsync(string schemaName)
    {
        await using var cmd = theConnection.CreateCommand($"DROP DATABASE IF EXISTS `{schemaName}`");
        await cmd.ExecuteNonQueryAsync();
    }

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
    public async Task InitializeAsync()
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

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
}
