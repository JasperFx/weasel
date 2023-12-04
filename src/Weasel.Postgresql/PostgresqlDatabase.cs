using JasperFx.Core;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Functions;

namespace Weasel.Postgresql;

public abstract class PostgresqlDatabase: DatabaseBase<NpgsqlConnection>
{
    /// <summary>
    /// Provide status of whether the Npgsql types were reloaded
    /// Currently used for assertion in unit tests
    /// </summary>
    public bool HasNpgsqlTypesReloaded { get; private set; }

    protected PostgresqlDatabase(
        IMigrationLogger logger,
        AutoCreate autoCreate,
        PostgresqlMigrator migrator,
        string identifier,
        string connectionString
    ): base(logger, autoCreate, migrator, identifier, connectionString)
    {
    }

    protected PostgresqlDatabase(
        IMigrationLogger logger,
        AutoCreate autoCreate,
        PostgresqlMigrator migrator,
        string identifier,
        Func<NpgsqlConnection> connectionSource
    ): base(logger, autoCreate, migrator, identifier, connectionSource)
    {
    }

    public async Task<Function?> DefinitionForFunction(DbObjectName function, CancellationToken ct = default)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(ct).ConfigureAwait(false);

        return await conn.FindExistingFunction(function, ct: ct).ConfigureAwait(false);
    }

    /// <summary>
    ///     Fetch a list of the existing tables in the database
    /// </summary>
    /// <param name="database"></param>
    /// <returns></returns>
    public async Task<IReadOnlyList<DbObjectName>> SchemaTables(CancellationToken ct = default)
    {
        var schemaNames = AllSchemaNames();

        await using var conn = CreateConnection();

        await conn.OpenAsync(ct).ConfigureAwait(false);

        return await conn.ExistingTablesAsync(schemas: schemaNames, ct: ct).ConfigureAwait(false);
    }

    public override async Task<SchemaPatchDifference> ApplyAllConfiguredChangesToDatabaseAsync(AutoCreate? @override = null,
        ReconnectionOptions? reconnectionOptions = null, CancellationToken ct = default)
    {
        HasNpgsqlTypesReloaded = false;
        var schemaPatchDiff =  await base.ApplyAllConfiguredChangesToDatabaseAsync(@override, reconnectionOptions, ct).ConfigureAwait(false);

        // If there were extensions installed, automatically reload Npgsql types cache.
        var hasExtensions = AllObjects().OfType<Extension>().Any();
        if (hasExtensions)
        {
            await ReloadNpgsqlTypes(ct).ConfigureAwait(false);
        }

        return schemaPatchDiff;
    }

    private async Task ReloadNpgsqlTypes(CancellationToken ct = default)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(ct).ConfigureAwait(false);
        await conn.ReloadTypesAsync().ConfigureAwait(false);
        HasNpgsqlTypesReloaded = true;
    }
}
