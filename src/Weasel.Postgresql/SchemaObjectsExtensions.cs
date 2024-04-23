using System.Data;
using System.Data.Common;
using JasperFx.Core;
using Npgsql;
using Weasel.Core;
using Weasel.Postgresql.Functions;

namespace Weasel.Postgresql;

public static class SchemaObjectsExtensions
{
    public static Task<Function?> FindExistingFunction(
        this NpgsqlConnection conn,
        DbObjectName functionName,
        CancellationToken ct = default
    )
    {
        var function = new Function(functionName, null);
        return function.FetchExistingAsync(conn, ct);
    }


    internal static string ToIndexName(this DbObjectName name, string prefix, params string[] columnNames)
    {
        return $"{prefix}_{name.Name}_{columnNames.Join("_")}";
    }

    public static async Task ApplyChangesAsync(this ISchemaObject schemaObject, NpgsqlConnection conn,
        CancellationToken ct = default)
    {
        var migration = await SchemaMigration.DetermineAsync(conn, ct, schemaObject).ConfigureAwait(false);

        await new PostgresqlMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct)
            .ConfigureAwait(false);
    }

    public static Task DropAsync(this ISchemaObject schemaObject, NpgsqlConnection conn, CancellationToken ct = default)
    {
        var writer = new StringWriter();
        schemaObject.WriteDropStatement(new PostgresqlMigrator(), writer);

        return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync(ct);
    }

    public static Task CreateAsync(
        this ISchemaObject schemaObject,
        NpgsqlConnection conn,
        CancellationToken ct = default
    )
    {
        var writer = new StringWriter();
        schemaObject.WriteCreateStatement(new PostgresqlMigrator(), writer);

        return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync(ct);
    }

    public static async Task EnsureSchemaExists(this NpgsqlConnection conn, string schemaName,
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
            await conn
                .CreateCommand(PostgresqlMigrator.CreateSchemaStatementFor(schemaName))
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
        this NpgsqlConnection conn,
        CancellationToken ct = default
    )
    {
        return conn.CreateCommand("select nspname from pg_catalog.pg_namespace order by nspname")
            .FetchListAsync<string>(cancellation: ct);
    }


    public static async Task DropSchemaAsync(this NpgsqlConnection conn, string schemaName,
        CancellationToken ct = default)
    {
        if (conn.State == ConnectionState.Closed)
        {
            await conn.OpenAsync(ct).ConfigureAwait(false);
        }

        await conn.CreateCommand(DropStatementFor(schemaName)).ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public static string DropStatementFor(string schemaName, CascadeAction option = CascadeAction.Cascade)
    {
        return $"drop schema if exists {schemaName} {option.ToString().ToUpperInvariant()};";
    }

    public static Task CreateSchemaAsync(this NpgsqlConnection conn, string schemaName, CancellationToken ct = default)
    {
        return conn.CreateCommand(PostgresqlMigrator.CreateSchemaStatementFor(schemaName)).ExecuteNonQueryAsync(ct);
    }

    public static Task ResetSchemaAsync(this NpgsqlConnection conn, string schemaName, CancellationToken ct = default)
    {
        return conn.RunSqlAsync(ct,
            DropStatementFor(schemaName),
            PostgresqlMigrator.CreateSchemaStatementFor(schemaName)
        );
    }

    public static async Task<bool> FunctionExistsAsync(
        this NpgsqlConnection conn,
        DbObjectName functionIdentifier,
        CancellationToken ct = default
    )
    {
        var sql =
            "SELECT specific_schema, routine_name FROM information_schema.routines WHERE type_udt_name != 'trigger' and routine_name like :name and specific_schema = :schema;";

        await using var reader = await conn.CreateCommand(sql)
            .With("name", functionIdentifier.Name)
            .With("schema", functionIdentifier.Schema)
            .ExecuteReaderAsync(ct).ConfigureAwait(false);

        var result = await reader.ReadAsync(ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return result;
    }

    public static async Task<IReadOnlyList<DbObjectName>> ExistingTablesAsync(
        this NpgsqlConnection conn,
        string? namePattern = null,
        string[]? schemas = null,
        CancellationToken ct = default
    )
    {
        var builder = new CommandBuilder();
        builder.Append("SELECT schemaname, relname FROM pg_stat_user_tables");

        var hasWhere = false;

        if (namePattern.IsNotEmpty())
        {
            builder.Append(" WHERE relname like :table");
            builder.AddNamedParameter("table", namePattern);

            if (schemas != null)
            {
                builder.Append(" and schemaname = ANY(:schemas)");
                builder.AddNamedParameter("schemas", schemas);
            }
        }
        else if (schemas != null)
        {
            builder.Append(" WHERE schemaname = ANY(:schemas)");
            builder.AddNamedParameter("schemas", schemas);
        }

        builder.Append(";");

        return await conn.FetchListAsync(builder, ReadDbObjectNameAsync, ct: ct).ConfigureAwait(false);
    }

    public static async Task<IReadOnlyList<DbObjectName>> ExistingFunctionsAsync(
        this NpgsqlConnection conn,
        string? namePattern = null,
        string[]? schemas = null,
        CancellationToken ct = default
    )
    {
        var builder = new CommandBuilder();
        builder.Append(
            "SELECT specific_schema, routine_name FROM information_schema.routines WHERE type_udt_name != 'trigger'");

        if (namePattern.IsNotEmpty())
        {
            builder.Append(" and routine_name like :name");
            builder.AddNamedParameter("name", namePattern);
        }

        if (schemas != null)
        {
            builder.Append(" and specific_schema = ANY(:schemas)");
            builder.AddNamedParameter("schemas", schemas);
        }

        builder.Append(";");

        return await conn.FetchListAsync(builder, ReadDbObjectNameAsync, ct: ct).ConfigureAwait(false);
    }

    private static async Task<DbObjectName> ReadDbObjectNameAsync(DbDataReader reader, CancellationToken ct = default)
    {
        return new PostgresqlObjectName(
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
    public static string ToCreateSql(this ISchemaObject @object, PostgresqlMigrator rules)
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
    public static async Task<bool> MigrateAsync(this ISchemaObject schemaObject, NpgsqlConnection conn, CancellationToken? cancellationToken = default, AutoCreate autoCreate = AutoCreate.CreateOrUpdate)
    {
        cancellationToken ??= CancellationToken.None;

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(cancellationToken.Value).ConfigureAwait(false);
        }

        var migration = await SchemaMigration.DetermineAsync(conn, cancellationToken.Value, schemaObject).ConfigureAwait(false);
        if (migration.Difference == SchemaPatchDifference.None) return false;

        migration.AssertPatchingIsValid(autoCreate);

        var migrator = new PostgresqlMigrator();
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
    public static async Task<bool> MigrateAsync(this ISchemaObject[] schemaObjects, NpgsqlConnection conn, CancellationToken? cancellationToken = default, AutoCreate autoCreate = AutoCreate.CreateOrUpdate)
    {
        cancellationToken ??= CancellationToken.None;

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(cancellationToken.Value).ConfigureAwait(false);
        }

        var migration = await SchemaMigration.DetermineAsync(conn, cancellationToken.Value, schemaObjects).ConfigureAwait(false);
        if (migration.Difference == SchemaPatchDifference.None) return false;

        migration.AssertPatchingIsValid(autoCreate);

        var migrator = new PostgresqlMigrator();
        await migrator.ApplyAllAsync(conn, migration, autoCreate, ct: cancellationToken.Value).ConfigureAwait(false);

        return true;
    }
}
