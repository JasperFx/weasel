using System.Data;
using System.Data.Common;
using JasperFx;
using JasperFx.Core;
using Microsoft.Data.Sqlite;
using Weasel.Core;

namespace Weasel.Sqlite;

public static class SchemaObjectsExtensions
{
    internal static string ToIndexName(this DbObjectName name, string prefix, params string[] columnNames)
    {
        return $"{prefix}_{name.Name}_{columnNames.Join("_")}";
    }

    public static async Task ApplyChangesAsync(
        this ISchemaObject schemaObject,
        SqliteConnection conn,
        CancellationToken ct = default
    )
    {
        var migration = await SchemaMigration.DetermineAsync(conn, ct, schemaObject).ConfigureAwait(false);

        await new SqliteMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct)
            .ConfigureAwait(false);
    }

    public static Task DropAsync(this ISchemaObject schemaObject, SqliteConnection conn, CancellationToken ct = default)
    {
        var writer = new StringWriter();
        schemaObject.WriteDropStatement(new SqliteMigrator(), writer);

        return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync(ct);
    }

    public static Task CreateAsync(
        this ISchemaObject schemaObject,
        SqliteConnection conn,
        CancellationToken ct = default
    )
    {
        var writer = new StringWriter();
        schemaObject.WriteCreateStatement(new SqliteMigrator(), writer);

        return conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// SQLite doesn't have separate schemas like PostgreSQL/SQL Server.
    /// This method is a no-op for SQLite but provided for API compatibility.
    /// SQLite only uses the "main" schema.
    /// </summary>
    public static Task EnsureSchemaExists(
        this SqliteConnection conn,
        string schemaName,
        CancellationToken cancellation = default
    )
    {
        // SQLite doesn't support CREATE SCHEMA. The main database is the default schema.
        // This is a no-op for API compatibility.
        return Task.CompletedTask;
    }

    /// <summary>
    /// Returns active schema names in SQLite. Typically returns "main" and "temp".
    /// </summary>
    public static Task<IReadOnlyList<string?>> ActiveSchemaNamesAsync(
        this SqliteConnection conn,
        CancellationToken ct = default
    )
    {
        return conn.CreateCommand("SELECT name FROM pragma_database_list ORDER BY name")
            .FetchListAsync<string>(cancellation: ct);
    }

    /// <summary>
    /// SQLite doesn't have separate schemas like PostgreSQL/SQL Server.
    /// This drops all tables, views, indexes, and triggers in the specified database (schema).
    /// For the "main" database, it drops all user objects but doesn't drop the database itself.
    /// </summary>
    public static async Task DropSchemaAsync(
        this SqliteConnection conn,
        string schemaName,
        CancellationToken ct = default
    )
    {
        if (conn.State == ConnectionState.Closed)
        {
            await conn.OpenAsync(ct).ConfigureAwait(false);
        }

        // Get all tables
        var tables = await conn
            .CreateCommand($"SELECT name FROM {SchemaUtils.QuoteName(schemaName)}.sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            .FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        // Get all views
        var views = await conn
            .CreateCommand($"SELECT name FROM {SchemaUtils.QuoteName(schemaName)}.sqlite_master WHERE type = 'view' ORDER BY name")
            .FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        // Get all indexes (excluding auto-created indexes)
        var indexes = await conn
            .CreateCommand($"SELECT name FROM {SchemaUtils.QuoteName(schemaName)}.sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%' ORDER BY name")
            .FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        // Get all triggers
        var triggers = await conn
            .CreateCommand($"SELECT name FROM {SchemaUtils.QuoteName(schemaName)}.sqlite_master WHERE type = 'trigger' ORDER BY name")
            .FetchListAsync<string>(cancellation: ct).ConfigureAwait(false);

        var drops = new List<string>();

        // Drop in proper order: triggers, views, indexes, tables
        drops.AddRange(triggers.Select(name =>
            $"DROP TRIGGER IF EXISTS {SchemaUtils.QuoteName(schemaName)}.{SchemaUtils.QuoteName(name)};"));
        drops.AddRange(views.Select(name =>
            $"DROP VIEW IF EXISTS {SchemaUtils.QuoteName(schemaName)}.{SchemaUtils.QuoteName(name)};"));
        drops.AddRange(indexes.Select(name =>
            $"DROP INDEX IF EXISTS {SchemaUtils.QuoteName(schemaName)}.{SchemaUtils.QuoteName(name)};"));
        drops.AddRange(tables.Select(name =>
            $"DROP TABLE IF EXISTS {SchemaUtils.QuoteName(schemaName)}.{SchemaUtils.QuoteName(name)};"));

        foreach (var drop in drops)
        {
            await conn.CreateCommand(drop).ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        // Note: We don't detach the database as that would require DETACH DATABASE
        // and is beyond the scope of dropping schema objects
    }

    /// <summary>
    /// SQLite doesn't support CREATE SCHEMA. This is a no-op for API compatibility.
    /// </summary>
    public static Task CreateSchemaAsync(this SqliteConnection conn, string schemaName, CancellationToken ct = default)
    {
        // SQLite doesn't support CREATE SCHEMA
        // This is a no-op for API compatibility
        return Task.CompletedTask;
    }

    /// <summary>
    /// Drops all objects in the specified schema and recreates it (no-op for the recreation part in SQLite).
    /// </summary>
    public static async Task ResetSchemaAsync(this SqliteConnection conn, string schemaName, CancellationToken ct = default)
    {
        await conn.DropSchemaAsync(schemaName, ct).ConfigureAwait(false);
        await conn.CreateSchemaAsync(schemaName, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// SQLite doesn't store user-defined functions in the database.
    /// Functions are registered programmatically per connection.
    /// This method always returns false.
    /// </summary>
    public static Task<bool> FunctionExistsAsync(
        this SqliteConnection conn,
        DbObjectName functionIdentifier,
        CancellationToken ct = default
    )
    {
        // SQLite doesn't store user-defined functions in the database
        // Functions are registered programmatically via CreateFunction/CreateAggregate
        return Task.FromResult(false);
    }

    public static async Task<IReadOnlyList<DbObjectName>> ExistingTablesAsync(
        this SqliteConnection conn,
        string? namePattern = null,
        string[]? schemas = null,
        CancellationToken ct = default
    )
    {
        // Default to "main" schema if none specified
        var schemaList = schemas ?? new[] { "main" };
        var results = new List<DbObjectName>();

        foreach (var schema in schemaList)
        {
            var builder = new CommandBuilder();
            builder.Append($"SELECT name FROM {SchemaUtils.QuoteName(schema)}.sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'");

            if (namePattern.IsNotEmpty())
            {
                builder.Append(" AND name LIKE @table");
                builder.AddNamedParameter("table", namePattern);
            }

            builder.Append(" ORDER BY name;");

            var schemaResults = await conn.FetchListAsync(builder, async (reader, ct) =>
                new SqliteObjectName(schema, await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false)),
                ct: ct).ConfigureAwait(false);
            results.AddRange(schemaResults);
        }

        return results;
    }

    public static async Task<IReadOnlyList<DbObjectName>> ExistingViewsAsync(
        this SqliteConnection conn,
        string? namePattern = null,
        string[]? schemas = null,
        CancellationToken ct = default
    )
    {
        // Default to "main" schema if none specified
        var schemaList = schemas ?? new[] { "main" };
        var results = new List<DbObjectName>();

        foreach (var schema in schemaList)
        {
            var builder = new CommandBuilder();
            builder.Append($"SELECT name FROM {SchemaUtils.QuoteName(schema)}.sqlite_master WHERE type = 'view'");

            if (namePattern.IsNotEmpty())
            {
                builder.Append(" AND name LIKE @view");
                builder.AddNamedParameter("view", namePattern);
            }

            builder.Append(" ORDER BY name;");

            var schemaResults = await conn.FetchListAsync(builder, async (reader, ct) =>
                new SqliteObjectName(schema, await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false)),
                ct: ct).ConfigureAwait(false);
            results.AddRange(schemaResults);
        }

        return results;
    }

    /// <summary>
    /// SQLite doesn't store user-defined functions in the database.
    /// This method always returns an empty list.
    /// </summary>
    public static Task<IReadOnlyList<DbObjectName>> ExistingFunctionsAsync(
        this SqliteConnection conn,
        string? namePattern = null,
        string[]? schemas = null,
        CancellationToken ct = default
    )
    {
        // SQLite doesn't store user-defined functions in sqlite_master
        // Functions are registered programmatically via CreateFunction/CreateAggregate
        return Task.FromResult<IReadOnlyList<DbObjectName>>(Array.Empty<DbObjectName>());
    }

    /// <summary>
    /// Write the creation SQL for this ISchemaObject
    /// </summary>
    /// <param name="object"></param>
    /// <param name="rules"></param>
    /// <returns></returns>
    public static string ToCreateSql(this ISchemaObject @object, SqliteMigrator rules)
    {
        var writer = new StringWriter();
        @object.WriteCreateStatement(rules, writer);

        return writer.ToString();
    }

    /// <summary>
    /// Perform any necessary migrations against a database for a supplied schema object
    /// </summary>
    /// <param name="schemaObject">A single schema object to be migrated</param>
    /// <param name="conn">A connection to the database you want to migrate. This method will open the connection if it is not already</param>
    /// <param name="cancellationToken"></param>
    /// <param name="autoCreate">Optionally override the AutoCreate settings, the default is CreateOrUpdate</param>
    /// <returns>True if there was a migration made, false if no changes were detected</returns>
    public static async Task<bool> MigrateAsync(
        this ISchemaObject schemaObject,
        SqliteConnection conn,
        CancellationToken? cancellationToken = default,
        AutoCreate autoCreate = AutoCreate.CreateOrUpdate
    )
    {
        cancellationToken ??= CancellationToken.None;

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(cancellationToken.Value).ConfigureAwait(false);
        }

        var migration = await SchemaMigration.DetermineAsync(conn, cancellationToken.Value, schemaObject)
            .ConfigureAwait(false);
        if (migration.Difference == SchemaPatchDifference.None)
        {
            return false;
        }

        migration.AssertPatchingIsValid(autoCreate);

        var migrator = new SqliteMigrator();
        await migrator.ApplyAllAsync(conn, migration, autoCreate, ct: cancellationToken.Value).ConfigureAwait(false);

        return true;
    }

    /// <summary>
    /// Perform any necessary migrations against a database for a supplied collection of schema objects
    /// </summary>
    /// <param name="schemaObjects">A collection of schema objects to migrate</param>
    /// <param name="conn">A connection to the database you want to migrate. This method will open the connection if it is not already</param>
    /// <param name="cancellationToken"></param>
    /// <param name="autoCreate">Optionally override the AutoCreate settings, the default is CreateOrUpdate</param>
    /// <returns>True if there was a migration made, false if no changes were detected</returns>
    public static async Task<bool> MigrateAsync(
        this ISchemaObject[] schemaObjects,
        SqliteConnection conn,
        CancellationToken? cancellationToken = default,
        AutoCreate autoCreate = AutoCreate.CreateOrUpdate
    )
    {
        cancellationToken ??= CancellationToken.None;

        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(cancellationToken.Value).ConfigureAwait(false);
        }

        var migration = await SchemaMigration.DetermineAsync(conn, cancellationToken.Value, schemaObjects)
            .ConfigureAwait(false);
        if (migration.Difference == SchemaPatchDifference.None)
        {
            return false;
        }

        migration.AssertPatchingIsValid(autoCreate);

        var migrator = new SqliteMigrator();
        await migrator.ApplyAllAsync(conn, migration, autoCreate, ct: cancellationToken.Value).ConfigureAwait(false);

        return true;
    }
}
