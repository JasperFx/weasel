using System.Data;
using System.Data.Common;
using JasperFx;
using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Weasel.Core;

namespace Weasel.SqlServer;

public static class SchemaObjectsExtensions
{
    internal static string ToIndexName(this DbObjectName name, string prefix, params string[] columnNames)
    {
        return $"{prefix}_{name.Name}_{columnNames.Join("_")}";
    }

    public static async Task ApplyChangesAsync(
        this ISchemaObject schemaObject,
        SqlConnection conn,
        CancellationToken ct = default
    )
    {
        var migration = await SchemaMigration.DetermineAsync(conn, ct, schemaObject).ConfigureAwait(false);

        await new SqlServerMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct)
            .ConfigureAwait(false);
    }

    public static Task Drop(this ISchemaObject schemaObject, SqlConnection conn, CancellationToken ct = default)
    {
        var writer = new StringWriter();
        schemaObject.WriteDropStatement(new SqlServerMigrator(), writer);

        return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync(ct);
    }

    public static Task CreateAsync(this ISchemaObject schemaObject, SqlConnection conn, CancellationToken ct = default)
    {
        var writer = new StringWriter();
        schemaObject.WriteCreateStatement(new SqlServerMigrator(), writer);

        return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync(ct);
    }

    public static async Task EnsureSchemaExists(this SqlConnection conn, string schemaName,
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
            var sql = $@"
IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'{schemaName}' )
    EXEC('CREATE SCHEMA [{schemaName}]');

";

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
        this SqlConnection conn,
        CancellationToken ct = default
    )
    {
        return conn.CreateCommand("select name from sys.schemas order by name")
            .FetchListAsync<string>(cancellation: ct);
    }


    public static async Task DropSchemaAsync(this SqlConnection conn, string schemaName, CancellationToken ct = default)
    {
        var procedures = await conn
            .CreateCommand(
                $"select routine_name from information_schema.routines where routine_schema = '{schemaName}' and routine_type = 'PROCEDURE';")
            .FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        var functions = await conn
            .CreateCommand(
                $"select routine_name from information_schema.routines where routine_schema = '{schemaName}' and routine_type = 'FUNCTION';")
            .FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        var constraints = await conn
            .CreateCommand(
                $"select table_name, constraint_name from information_schema.table_constraints where table_schema = '{schemaName}' order by constraint_type")
            .FetchListAsync<string>(async r =>
            {
                var tableName = await r.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
                var constraintName = await r.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);

                return $"alter table {schemaName}.{tableName} drop constraint {constraintName};";
            }, cancellation: ct).ConfigureAwait(false);

        var tables = await conn
            .CreateCommand($"select table_name from information_schema.tables where table_schema = '{schemaName}'")
            .FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        var sequences = await conn
            .CreateCommand(
                $"select sequence_name from information_schema.sequences where sequence_schema = '{schemaName}'")
            .FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        var tableTypes = await conn
            .CreateCommand(
                $"select sys.table_types.name from sys.table_types inner join sys.schemas on sys.table_types.schema_id = sys.schemas.schema_id where sys.schemas.name = '{schemaName}'")
            .FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        var drops = new List<string>();
        drops.AddRange(procedures.Select(name => $"drop procedure {schemaName}.{name};"));
        drops.AddRange(functions.Select(name => $"drop function {schemaName}.{name};"));
        drops.AddRange(constraints);
        drops.AddRange(tables.Select(name => $"drop table {schemaName}.{name};"));
        drops.AddRange(sequences.Select(name => $"drop sequence {schemaName}.{name};"));
        drops.AddRange(tableTypes.Select(x => $"DROP TYPE {schemaName}.{x};"));


        foreach (var drop in drops) await conn.CreateCommand(drop).ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        if (!schemaName.EqualsIgnoreCase(SqlServerProvider.Instance.DefaultDatabaseSchemaName))
        {
            var sql = $"drop schema if exists {schemaName};";
            await conn.CreateCommand(sql).ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    public static Task CreateSchemaAsync(this SqlConnection conn, string schemaName, CancellationToken ct = default)
    {
        return conn.CreateCommand(SqlServerMigrator.CreateSchemaStatementFor(schemaName)).ExecuteNonQueryAsync(ct);
    }

    public static async Task ResetSchemaAsync(this SqlConnection conn, string schemaName,
        CancellationToken ct = default)
    {
        try
        {
            await conn.DropSchemaAsync(schemaName, ct: ct).ConfigureAwait(false);
        }
        catch (SqlException e)
        {
            if (e.Message.Contains("deadlocked"))
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

        await conn.RunSqlAsync(ct, SqlServerMigrator.CreateSchemaStatementFor(schemaName)).ConfigureAwait(false);
    }

    public static async Task<bool> FunctionExistsAsync(
        this SqlConnection conn,
        DbObjectName functionIdentifier,
        CancellationToken ct = default
    )
    {
        var sql = "SELECT 1 FROM information_schema.routines WHERE routine_name = @name and specific_schema = @schema;";
        await using var reader = await conn.CreateCommand(sql)
            .With("name", functionIdentifier.Name)
            .With("schema", functionIdentifier.Schema)
            .ExecuteReaderAsync(ct).ConfigureAwait(false);

        var result = await reader.ReadAsync(ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return result;
    }

    public static async Task<IReadOnlyList<DbObjectName>> ExistingTables(
        this SqlConnection conn,
        string? namePattern = null,
        CancellationToken ct = default
    )
    {
        var builder = new CommandBuilder();
        builder.Append("SELECT table_schema, table_name FROM information_schema.tables");


        if (namePattern.IsNotEmpty())
        {
            builder.Append(" WHERE table_name like @table");
            builder.AddNamedParameter("table", namePattern);
        }

        builder.Append(";");

        return await conn.FetchListAsync(builder, ReadDbObjectNameAsync, ct).ConfigureAwait(false);
    }

    public static async Task<IReadOnlyList<DbObjectName>> ExistingFunctionsAsync(
        this SqlConnection conn,
        string? namePattern = null,
        string[]? schemas = null,
        CancellationToken ct = default
    )
    {
        var builder = new CommandBuilder();
        builder.Append(
            "SELECT specific_schema, routine_name FROM information_schema.routines WHERE routine_type = 'FUNCTION'");

        if (namePattern.IsNotEmpty())
        {
            builder.Append(" and routine_name like @name");
            builder.AddNamedParameter("@name", namePattern);
        }

        if (schemas != null && schemas.Any())
        {
            builder.Append(" and specific_schema = @schemas");
            builder.AddNamedParameter("schemas", schemas[0]);
        }

        builder.Append(";");

        return await conn.FetchListAsync(builder, ReadDbObjectNameAsync, ct).ConfigureAwait(false);
    }

    private static async Task<DbObjectName> ReadDbObjectNameAsync(DbDataReader reader, CancellationToken ct = default)
    {
        return new SqlServerObjectName(
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
    public static string ToCreateSql(this ISchemaObject @object, SqlServerMigrator rules)
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
    public static async Task<bool> MigrateAsync(this ISchemaObject schemaObject, SqlConnection conn, CancellationToken? cancellationToken = default, AutoCreate autoCreate = AutoCreate.CreateOrUpdate)
    {
        cancellationToken ??= CancellationToken.None;

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(cancellationToken.Value).ConfigureAwait(false);
        }

        var migration = await SchemaMigration.DetermineAsync(conn, cancellationToken.Value, schemaObject).ConfigureAwait(false);
        if (migration.Difference == SchemaPatchDifference.None) return false;

        migration.AssertPatchingIsValid(autoCreate);

        var migrator = new SqlServerMigrator();
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
    public static async Task<bool> MigrateAsync(this ISchemaObject[] schemaObjects,SqlConnection conn, CancellationToken? cancellationToken = default, AutoCreate autoCreate = AutoCreate.CreateOrUpdate)
    {
        cancellationToken ??= CancellationToken.None;

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(cancellationToken.Value).ConfigureAwait(false);
        }

        var migration = await SchemaMigration.DetermineAsync(conn, cancellationToken.Value, schemaObjects).ConfigureAwait(false);
        if (migration.Difference == SchemaPatchDifference.None) return false;

        migration.AssertPatchingIsValid(autoCreate);

        var migrator = new SqlServerMigrator();
        await migrator.ApplyAllAsync(conn, migration, autoCreate, ct: cancellationToken.Value).ConfigureAwait(false);

        return true;
    }
}
