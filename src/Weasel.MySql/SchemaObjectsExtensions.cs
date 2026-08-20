using System.Data;
using JasperFx;
using MySqlConnector;
using Weasel.Core;

namespace Weasel.MySql;

public static class SchemaObjectsExtensions
{
    public static async Task ApplyChangesAsync(
        this ISchemaObject schemaObject,
        MySqlConnection connection,
        CancellationToken ct = default)
    {
        var migration = await SchemaMigration.DetermineAsync(connection, ct, schemaObject).ConfigureAwait(false);
        await new MySqlMigrator().ApplyAllAsync(connection, migration, AutoCreate.CreateOrUpdate, ct: ct)
            .ConfigureAwait(false);
    }

    public static async Task CreateAsync(
        this ISchemaObject schemaObject,
        MySqlConnection connection,
        CancellationToken ct = default)
    {
        var writer = new StringWriter();
        schemaObject.WriteCreateStatement(new MySqlMigrator(), writer);

        var sql = writer.ToString();

        // MySQL doesn't support multiple statements by default, split them
        var statements = sql.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
        foreach (var statement in statements)
        {
            var trimmed = statement.Trim();
            if (string.IsNullOrEmpty(trimmed)) continue;

            await using var cmd = connection.CreateCommand(trimmed);
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    public static async Task DropAsync(
        this ISchemaObject schemaObject,
        MySqlConnection connection,
        CancellationToken ct = default)
    {
        var writer = new StringWriter();
        schemaObject.WriteDropStatement(new MySqlMigrator(), writer);

        var sql = writer.ToString();
        await using var cmd = connection.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    ///     Drop a MySQL schema and everything in it.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         A MySQL schema is a database, so this is <c>DROP DATABASE IF EXISTS</c> and the
    ///         server does the cascade. Like PostgreSQL's, and unlike the three providers that
    ///         enumerate object types by hand, it is <em>complete by construction</em>: a new
    ///         creatable object type can never leave it behind (weasel#465).
    ///     </para>
    ///     <para>
    ///         MySQL was the one provider of the five with no schema extension at all — the drop
    ///         existed only inside <c>MySqlMigrator.WriteSchemaDropSql</c> and as a private helper
    ///         in the test context. This exists for symmetry with the other four.
    ///     </para>
    /// </remarks>
    public static async Task DropSchemaAsync(
        this MySqlConnection conn,
        string schemaName,
        CancellationToken ct = default)
    {
        if (conn.State == ConnectionState.Closed)
        {
            await conn.OpenAsync(ct).ConfigureAwait(false);
        }

        await conn.CreateCommand(DropStatementFor(schemaName)).ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public static string DropStatementFor(string schemaName)
        => $"DROP DATABASE IF EXISTS {SchemaUtils.QuoteName(schemaName)};";

    public static async Task CreateSchemaAsync(
        this MySqlConnection conn,
        string schemaName,
        CancellationToken ct = default)
    {
        if (conn.State == ConnectionState.Closed)
        {
            await conn.OpenAsync(ct).ConfigureAwait(false);
        }

        await conn.CreateCommand(MySqlMigrator.CreateDatabaseStatementFor(schemaName))
            .ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public static async Task ResetSchemaAsync(
        this MySqlConnection conn,
        string schemaName,
        CancellationToken ct = default)
    {
        await conn.DropSchemaAsync(schemaName, ct).ConfigureAwait(false);
        await conn.CreateSchemaAsync(schemaName, ct).ConfigureAwait(false);
    }
}
