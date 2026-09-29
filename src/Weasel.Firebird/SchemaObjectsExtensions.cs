using System.Data;
using FirebirdSql.Data.FirebirdClient;
using JasperFx;
using Weasel.Core;

namespace Weasel.Firebird;

public static partial class SchemaObjectsExtensions
{
    /// <summary>
    ///     Bring <paramref name="schemaObject" /> in line with the database, creating or updating it.
    /// </summary>
    public static async Task ApplyChangesAsync(
        this ISchemaObject schemaObject,
        FbConnection conn,
        CancellationToken ct = default
    )
    {
        await openAsync(conn, ct).ConfigureAwait(false);

        var migrator = new FirebirdMigrator();
        var migration = await SchemaMigration.DetermineAsync(conn, migrator, ct, schemaObject).ConfigureAwait(false);

        await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct).ConfigureAwait(false);
    }

    /// <summary>
    ///     Run <paramref name="schemaObject" />'s creation DDL, one statement per command.
    /// </summary>
    public static async Task CreateAsync(this ISchemaObject schemaObject, FbConnection conn, CancellationToken ct = default)
    {
        var migrator = new FirebirdMigrator();
        var writer = new StringWriter();
        schemaObject.WriteCreateStatement(migrator, writer);

        await openAsync(conn, ct).ConfigureAwait(false);
        await migrator.ExecuteScriptAsync(conn, writer.ToString(), ct).ConfigureAwait(false);
    }

    /// <summary>
    ///     Run <paramref name="schemaObject" />'s drop DDL, one statement per command.
    /// </summary>
    public static async Task DropAsync(this ISchemaObject schemaObject, FbConnection conn, CancellationToken ct = default)
    {
        var migrator = new FirebirdMigrator();
        var writer = new StringWriter();
        schemaObject.WriteDropStatement(migrator, writer);

        await openAsync(conn, ct).ConfigureAwait(false);
        await migrator.ExecuteScriptAsync(conn, writer.ToString(), ct).ConfigureAwait(false);
    }

    /// <summary>
    ///     Write the creation SQL for this ISchemaObject
    /// </summary>
    public static string ToCreateSql(this ISchemaObject @object, FirebirdMigrator rules)
    {
        var writer = new StringWriter();
        @object.WriteCreateStatement(rules, writer);

        return writer.ToString();
    }

    /// <summary>
    ///     Perform any necessary migrations against a database for a supplied schema object
    /// </summary>
    /// <returns>True if there was a migration made, false if no changes were detected</returns>
    public static Task<bool> MigrateAsync(this ISchemaObject schemaObject, FbConnection conn,
        CancellationToken? cancellationToken = default, AutoCreate autoCreate = AutoCreate.CreateOrUpdate)
        => new[] { schemaObject }.MigrateAsync(conn, cancellationToken, autoCreate);

    /// <summary>
    ///     Perform any necessary migrations against a database for a supplied number of schema objects
    /// </summary>
    /// <returns>True if there was a migration made, false if no changes were detected</returns>
    public static async Task<bool> MigrateAsync(this ISchemaObject[] schemaObjects, FbConnection conn,
        CancellationToken? cancellationToken = default, AutoCreate autoCreate = AutoCreate.CreateOrUpdate)
    {
        var ct = cancellationToken ?? CancellationToken.None;

        await openAsync(conn, ct).ConfigureAwait(false);

        var migrator = new FirebirdMigrator();
        var migration = await SchemaMigration.DetermineAsync(conn, migrator, ct, schemaObjects).ConfigureAwait(false);
        if (migration.Difference == SchemaPatchDifference.None)
        {
            return false;
        }

        migration.AssertPatchingIsValid(autoCreate);

        await migrator.ApplyAllAsync(conn, migration, autoCreate, ct: ct).ConfigureAwait(false);

        return true;
    }

    private static async Task openAsync(FbConnection conn, CancellationToken ct)
    {
        if (conn.State != ConnectionState.Open)
        {
            await conn.OpenAsync(ct).ConfigureAwait(false);
        }
    }
}
