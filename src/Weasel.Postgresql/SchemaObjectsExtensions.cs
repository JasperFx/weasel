using System.Data;
using System.Data.Common;
using JasperFx.Core;
using Npgsql;
using Weasel.Core;
using Weasel.Postgresql.Functions;

namespace Weasel.Postgresql;

public static class SchemaObjectsExtensions
{
    public static Task<Function?> FindExistingFunction(this NpgsqlConnection conn, DbObjectName functionName)
    {
        var function = new Function(functionName, null);
        return function.FetchExisting(conn);
    }


    internal static string ToIndexName(this DbObjectName name, string prefix, params string[] columnNames)
    {
        return $"{prefix}_{name.Name}_{columnNames.Join("_")}";
    }

    public static async Task ApplyChanges(this ISchemaObject schemaObject, NpgsqlConnection conn)
    {
        var migration = await SchemaMigration.Determine(conn, schemaObject).ConfigureAwait(false);

        await new PostgresqlMigrator().ApplyAll(conn, migration, AutoCreate.CreateOrUpdate).ConfigureAwait(false);
    }

    public static Task Drop(this ISchemaObject schemaObject, NpgsqlConnection conn)
    {
        var writer = new StringWriter();
        schemaObject.WriteDropStatement(new PostgresqlMigrator(), writer);

        return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync();
    }

    public static Task Create(this ISchemaObject schemaObject, NpgsqlConnection conn)
    {
        var writer = new StringWriter();
        schemaObject.WriteCreateStatement(new PostgresqlMigrator(), writer);

        return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync();
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

    public static Task<IReadOnlyList<string?>> ActiveSchemaNames(this NpgsqlConnection conn)
    {
        return conn.CreateCommand("select nspname from pg_catalog.pg_namespace order by nspname")
            .FetchList<string>();
    }


    public static async Task DropSchema(this NpgsqlConnection conn, string schemaName)
    {
        if (conn.State == ConnectionState.Closed)
        {
            await conn.OpenAsync().ConfigureAwait(false);
        }

        await conn.CreateCommand(DropStatementFor(schemaName)).ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    public static string DropStatementFor(string schemaName, CascadeAction option = CascadeAction.Cascade)
    {
        return $"drop schema if exists {schemaName} {option.ToString().ToUpperInvariant()};";
    }

    public static Task CreateSchema(this NpgsqlConnection conn, string schemaName)
    {
        return conn.CreateCommand(PostgresqlMigrator.CreateSchemaStatementFor(schemaName)).ExecuteNonQueryAsync();
    }

    public static Task ResetSchema(this NpgsqlConnection conn, string schemaName)
    {
        return conn.RunSql(DropStatementFor(schemaName), PostgresqlMigrator.CreateSchemaStatementFor(schemaName));
    }

    public static async Task<bool> FunctionExists(this NpgsqlConnection conn, DbObjectName functionIdentifier)
    {
        var sql =
            "SELECT specific_schema, routine_name FROM information_schema.routines WHERE type_udt_name != 'trigger' and routine_name like :name and specific_schema = :schema;";

        await using var reader = await conn.CreateCommand(sql)
            .With("name", functionIdentifier.Name)
            .With("schema", functionIdentifier.Schema)
            .ExecuteReaderAsync().ConfigureAwait(false);

        return await reader.ReadAsync().ConfigureAwait(false);
    }

    public static async Task<IReadOnlyList<DbObjectName>> ExistingTables(this NpgsqlConnection conn,
        string? namePattern = null, string[]? schemas = null)
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

        return await builder.FetchList(conn, ReadDbObjectName).ConfigureAwait(false);
    }

    public static async Task<IReadOnlyList<DbObjectName>> ExistingFunctions(this NpgsqlConnection conn,
        string? namePattern = null, string[]? schemas = null)
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

        return await builder.FetchList(conn, ReadDbObjectName).ConfigureAwait(false);
    }

    private static async Task<DbObjectName> ReadDbObjectName(DbDataReader reader)
    {
        return new DbObjectName(await reader.GetFieldValueAsync<string>(0).ConfigureAwait(false),
            await reader.GetFieldValueAsync<string>(1).ConfigureAwait(false));
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
}
