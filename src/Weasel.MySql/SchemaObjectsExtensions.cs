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
}
