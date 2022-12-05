using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Functions;

namespace Weasel.Postgresql;

public abstract class PostgresqlDatabase: DatabaseBase<NpgsqlConnection>
{
    protected PostgresqlDatabase(IMigrationLogger logger, AutoCreate autoCreate, PostgresqlMigrator migrator,
        string identifier, string connectionString): base(logger, autoCreate, migrator, identifier, connectionString)
    {
    }

    protected PostgresqlDatabase(IMigrationLogger logger, AutoCreate autoCreate, PostgresqlMigrator migrator,
        string identifier, Func<NpgsqlConnection> connectionSource): base(logger, autoCreate, migrator, identifier,
        connectionSource)
    {
    }

    public async Task<Function?> DefinitionForFunction(DbObjectName function)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync().ConfigureAwait(false);

        return await conn.FindExistingFunction(function).ConfigureAwait(false);
    }

    /// <summary>
    ///     Fetch a list of the existing tables in the database
    /// </summary>
    /// <param name="database"></param>
    /// <returns></returns>
    public async Task<IReadOnlyList<DbObjectName>> SchemaTables()
    {
        var schemaNames = AllSchemaNames();

        await using var conn = CreateConnection();

        await conn.OpenAsync().ConfigureAwait(false);

        return await conn.ExistingTables(schemas: schemaNames).ConfigureAwait(false);
    }
}
