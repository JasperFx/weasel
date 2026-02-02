using System.Data;
using System.Data.Common;
using JasperFx;
using JasperFx.Core;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;

namespace Weasel.Oracle;

public static class SchemaObjectsExtensions
{
    internal static string ToIndexName(this DbObjectName name, string prefix, params string[] columnNames)
    {
        return $"{prefix}_{name.Name}_{columnNames.Join("_")}";
    }

    public static async Task ApplyChangesAsync(
        this ISchemaObject schemaObject,
        OracleConnection conn,
        CancellationToken ct = default
    )
    {
        var migration = await SchemaMigration.DetermineAsync(conn, ct, schemaObject).ConfigureAwait(false);

        await new OracleMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct)
            .ConfigureAwait(false);
    }

    public static Task Drop(this ISchemaObject schemaObject, OracleConnection conn, CancellationToken ct = default)
    {
        var writer = new StringWriter();
        schemaObject.WriteDropStatement(new OracleMigrator(), writer);

        return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync(ct);
    }

    public static Task CreateAsync(this ISchemaObject schemaObject, OracleConnection conn, CancellationToken ct = default)
    {
        var writer = new StringWriter();
        schemaObject.WriteCreateStatement(new OracleMigrator(), writer);

        return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync(ct);
    }

    public static async Task EnsureSchemaExists(this OracleConnection conn, string schemaName,
        CancellationToken cancellation = default)
    {
        var shouldClose = false;
        if (conn.State != ConnectionState.Open)
        {
            shouldClose = true;
            await conn.OpenAsync(cancellation).ConfigureAwait(false);
        }

        try
        {
            var sql = OracleMigrator.CreateSchemaStatementFor(schemaName);

            await conn
                .CreateCommand(sql)
                .ExecuteNonQueryAsync(cancellation).ConfigureAwait(false);
        }
        finally
        {
            if (shouldClose)
            {
                await conn.CloseAsync().ConfigureAwait(false);
            }
        }
    }

    public static Task<IReadOnlyList<string?>> ActiveSchemaNamesAsync(
        this OracleConnection conn,
        CancellationToken ct = default
    )
    {
        return conn.CreateCommand("SELECT username FROM all_users ORDER BY username")
            .FetchListAsync<string>(cancellation: ct);
    }

    public static async Task DropSchemaAsync(this OracleConnection conn, string schemaName, CancellationToken ct = default)
    {
        var upperSchema = schemaName.ToUpperInvariant();

        var procedures = await conn
            .CreateCommand(
                $"SELECT object_name FROM all_objects WHERE owner = '{upperSchema}' AND object_type = 'PROCEDURE'")
            .FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        var functions = await conn
            .CreateCommand(
                $"SELECT object_name FROM all_objects WHERE owner = '{upperSchema}' AND object_type = 'FUNCTION'")
            .FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        var tables = await conn
            .CreateCommand($"SELECT table_name FROM all_tables WHERE owner = '{upperSchema}'")
            .FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        var sequences = await conn
            .CreateCommand(
                $"SELECT sequence_name FROM all_sequences WHERE sequence_owner = '{upperSchema}'")
            .FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        var drops = new List<string>();
        drops.AddRange(procedures.Select(name => $"DROP PROCEDURE {SchemaUtils.QuoteName(schemaName)}.{SchemaUtils.QuoteName(name!)}"));
        drops.AddRange(functions.Select(name => $"DROP FUNCTION {SchemaUtils.QuoteName(schemaName)}.{SchemaUtils.QuoteName(name!)}"));
        drops.AddRange(tables.Select(name => $"DROP TABLE {SchemaUtils.QuoteName(schemaName)}.{SchemaUtils.QuoteName(name!)} CASCADE CONSTRAINTS"));
        drops.AddRange(sequences.Select(name => $"DROP SEQUENCE {SchemaUtils.QuoteName(schemaName)}.{SchemaUtils.QuoteName(name!)}"));

        foreach (var drop in drops)
        {
            await conn.CreateCommand(drop).ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    public static Task CreateSchemaAsync(this OracleConnection conn, string schemaName, CancellationToken ct = default)
    {
        return conn.CreateCommand(OracleMigrator.CreateSchemaStatementFor(schemaName)).ExecuteNonQueryAsync(ct);
    }

    public static async Task ResetSchemaAsync(this OracleConnection conn, string schemaName,
        CancellationToken ct = default)
    {
        try
        {
            await conn.DropSchemaAsync(schemaName, ct: ct).ConfigureAwait(false);
        }
        catch (OracleException e)
        {
            if (e.Message.Contains("deadlock"))
            {
                await Task.Delay(100, ct).ConfigureAwait(false);
                await conn.CloseAsync().ConfigureAwait(false);
                await conn.OpenAsync(ct).ConfigureAwait(false);
                await conn.DropSchemaAsync(schemaName, ct: ct).ConfigureAwait(false);
            }
            else
            {
                throw;
            }
        }

        var sql = OracleMigrator.CreateSchemaStatementFor(schemaName);
        await conn.RunSqlAsync(ct, sql).ConfigureAwait(false);
    }

    public static async Task<bool> FunctionExistsAsync(
        this OracleConnection conn,
        DbObjectName functionIdentifier,
        CancellationToken ct = default
    )
    {
        var sql = "SELECT 1 FROM all_objects WHERE object_name = :name AND owner = :schema AND object_type = 'FUNCTION'";
        await using var reader = await conn.CreateCommand(sql)
            .With("name", functionIdentifier.Name.ToUpperInvariant())
            .With("schema", functionIdentifier.Schema.ToUpperInvariant())
            .ExecuteReaderAsync(ct).ConfigureAwait(false);

        var result = await reader.ReadAsync(ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return result;
    }

    public static async Task<IReadOnlyList<DbObjectName>> ExistingTables(
        this OracleConnection conn,
        string? namePattern = null,
        CancellationToken ct = default
    )
    {
        var builder = new CommandBuilder();
        builder.Append("SELECT owner, table_name FROM all_tables");

        if (namePattern.IsNotEmpty())
        {
            builder.Append(" WHERE table_name LIKE :table_pattern");
            builder.AddNamedParameter("table_pattern", namePattern.ToUpperInvariant());
        }

        return await conn.FetchListAsync(builder, ReadDbObjectNameAsync, ct).ConfigureAwait(false);
    }

    public static async Task<IReadOnlyList<DbObjectName>> ExistingFunctionsAsync(
        this OracleConnection conn,
        string? namePattern = null,
        string[]? schemas = null,
        CancellationToken ct = default
    )
    {
        var builder = new CommandBuilder();
        builder.Append(
            "SELECT owner, object_name FROM all_objects WHERE object_type = 'FUNCTION'");

        if (namePattern.IsNotEmpty())
        {
            builder.Append(" AND object_name LIKE :name_pattern");
            builder.AddNamedParameter("name_pattern", namePattern.ToUpperInvariant());
        }

        if (schemas != null && schemas.Any())
        {
            builder.Append(" AND owner = :owner");
            builder.AddNamedParameter("owner", schemas[0].ToUpperInvariant());
        }

        return await conn.FetchListAsync(builder, ReadDbObjectNameAsync, ct).ConfigureAwait(false);
    }

    private static async Task<DbObjectName> ReadDbObjectNameAsync(DbDataReader reader, CancellationToken ct = default)
    {
        return new OracleObjectName(
            await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false),
            await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false)
        );
    }

    /// <summary>
    ///     Write the creation SQL for this ISchemaObject
    /// </summary>
    /// <param name="object"></param>
    /// <param name="rules"></param>
    /// <returns></returns>
    public static string ToCreateSql(this ISchemaObject @object, OracleMigrator rules)
    {
        var writer = new StringWriter();
        @object.WriteCreateStatement(rules, writer);

        return writer.ToString();
    }

    /// <summary>
    /// Perform any necessary migrations against a database for a supplied number of schema objects
    /// </summary>
    /// <param name="conn">A connection to the database you want to migrate. This method will open the connection if it is not already</param>
    /// <param name="schemaObject">A single schema object to be migrated</param>
    /// <param name="cancellationToken"></param>
    /// <param name="autoCreate">Optionally override the AutoCreate settings, the default is CreateOrUpdate</param>
    /// <returns>True if there was a migration made, false if no changes were detected</returns>
    public static async Task<bool> MigrateAsync(this ISchemaObject schemaObject, OracleConnection conn, CancellationToken? cancellationToken = default, AutoCreate autoCreate = AutoCreate.CreateOrUpdate)
    {
        cancellationToken ??= CancellationToken.None;

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(cancellationToken.Value).ConfigureAwait(false);
        }

        var migration = await SchemaMigration.DetermineAsync(conn, cancellationToken.Value, schemaObject).ConfigureAwait(false);
        if (migration.Difference == SchemaPatchDifference.None) return false;

        migration.AssertPatchingIsValid(autoCreate);

        var migrator = new OracleMigrator();
        await migrator.ApplyAllAsync(conn, migration, autoCreate, ct: cancellationToken.Value).ConfigureAwait(false);

        return true;
    }

    /// <summary>
    /// Perform any necessary migrations against a database for a supplied number of schema objects
    /// </summary>
    /// <param name="conn">A connection to the database you want to migrate. This method will open the connection if it is not already</param>
    /// <param name="schemaObjects">A collection of schema objects to migrate</param>
    /// <param name="cancellationToken"></param>
    /// <param name="autoCreate">Optionally override the AutoCreate settings, the default is CreateOrUpdate</param>
    /// <returns></returns>
    public static async Task<bool> MigrateAsync(this ISchemaObject[] schemaObjects, OracleConnection conn, CancellationToken? cancellationToken = default, AutoCreate autoCreate = AutoCreate.CreateOrUpdate)
    {
        cancellationToken ??= CancellationToken.None;

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(cancellationToken.Value).ConfigureAwait(false);
        }

        var migration = await SchemaMigration.DetermineAsync(conn, cancellationToken.Value, schemaObjects).ConfigureAwait(false);
        if (migration.Difference == SchemaPatchDifference.None) return false;

        migration.AssertPatchingIsValid(autoCreate);

        var migrator = new OracleMigrator();
        await migrator.ApplyAllAsync(conn, migration, autoCreate, ct: cancellationToken.Value).ConfigureAwait(false);

        return true;
    }
}
