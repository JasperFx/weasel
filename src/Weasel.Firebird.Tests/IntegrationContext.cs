using FirebirdSql.Data.FirebirdClient;
using JasperFx;
using Weasel.Core;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     A database file of its own per test class, replaced before every test, so no test sees another's
///     objects and nothing needs tearing down by hand.
/// </summary>
/// <remarks>
///     The pools are cleared around every replacement: FirebirdClient does not validate a pooled
///     connection when it hands one out, and one left attached to the old file blocks replacing it.
/// </remarks>
[Collection("integration")]
public abstract class IntegrationContext: IAsyncLifetime
{
    protected FbConnection theConnection = null!;

    protected IntegrationContext(string? database = null, string? charset = null)
    {
        ConnectionString = ConnectionSource.ForDatabase(database ?? GetType().Name, charset);
    }

    protected string ConnectionString { get; }

    protected FirebirdServerVersion ServerVersion { get; private set; }

    public virtual async ValueTask InitializeAsync()
    {
        await ResetDatabaseAsync();
    }

    public virtual async ValueTask DisposeAsync()
    {
        if (theConnection != null)
        {
            await theConnection.DisposeAsync();
        }

        FbConnection.ClearAllPools();
    }

    protected async Task ResetDatabaseAsync()
    {
        if (theConnection != null)
        {
            await theConnection.DisposeAsync();
        }

        await ConnectionSource.CreateDatabaseAsync(ConnectionString);

        theConnection = new FbConnection(ConnectionString);
        await theConnection.OpenAsync();

        ServerVersion = FirebirdServerVersion.Of(theConnection)!.Value;
    }

    protected async Task<FbConnection> OpenConnectionAsync()
    {
        var conn = new FbConnection(ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    /// <summary>
    ///     Run DDL or a script the way a migration does: split, one statement per command.
    /// </summary>
    protected Task ExecuteAsync(string sql)
        => new FirebirdMigrator().ExecuteScriptAsync(theConnection, sql);

    protected async Task<T> ScalarAsync<T>(string sql)
    {
        await using var cmd = theConnection.CreateCommand(sql);
        var result = await cmd.ExecuteScalarAsync();
        return (T)Convert.ChangeType(result!, typeof(T));
    }

    protected async Task<List<string>> ListAsync(string sql)
    {
        var values = new List<string>();

        await using var cmd = theConnection.CreateCommand(sql);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetValue(0).ToString()!.Trim());
        }

        return values;
    }

    protected Task CreateSchemaObjectInDatabase(ISchemaObject schemaObject)
        => schemaObject.CreateAsync(theConnection);

    /// <summary>
    ///     Determine the migration for these objects through the migration path and apply it.
    /// </summary>
    protected async Task<SchemaMigration> ApplyAsync(AutoCreate autoCreate, params ISchemaObject[] objects)
    {
        var migrator = new FirebirdMigrator();
        var migration = await SchemaMigration.DetermineAsync(theConnection, migrator, CancellationToken.None, objects);
        await migrator.ApplyAllAsync(theConnection, migration, autoCreate);
        return migration;
    }

    protected Task<SchemaMigration> ApplyAsync(params ISchemaObject[] objects)
        => ApplyAsync(AutoCreate.CreateOrUpdate, objects);

    protected Task<SchemaMigration> DetermineAsync(params ISchemaObject[] objects)
        => SchemaMigration.DetermineAsync(theConnection, new FirebirdMigrator(), CancellationToken.None, objects);
}
