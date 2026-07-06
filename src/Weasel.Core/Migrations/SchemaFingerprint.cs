using System.Data.Common;
using System.Security.Cryptography;
using System.Text;

namespace Weasel.Core.Migrations;

/// <summary>
/// Opt-in short-circuit for <see cref="DatabaseBase{TConnection}.ApplyAllConfiguredChangesToDatabaseAsync(Weasel.Core.MultiTenancy.IGlobalLock{TConnection}, JasperFx.AutoCreate?, ReconnectionOptions?, System.Threading.CancellationToken)"/>
/// (see <see cref="Migrator.UseSchemaFingerprinting"/>): after a successful full apply, a SHA-256
/// fingerprint of the configured schema's expected DDL is stamped into a one-row table in the migrator's
/// default schema. The next full apply recomputes the fingerprint in memory and, when it matches the
/// stamp, returns without any catalog introspection — turning the no-op apply into a single SELECT.
///
/// <para>
/// The fingerprint covers everything the configuration would create — tables, indexes, functions,
/// managed partitions, sequences — because it hashes each schema object's <c>WriteCreateStatement</c>
/// output (sorted by identifier for stability). Any configuration change, including newly registered
/// tenant partitions, changes the hash and re-enables the full apply.
/// </para>
///
/// <para>
/// TRUST SEMANTICS: a matching stamp is trusted. Schema drift applied outside Weasel (manual DDL,
/// another tool) is not detected while the stamp matches — exactly like an application that skips
/// migrations altogether. <c>AssertDatabaseMatchesConfigurationAsync</c> remains the verification
/// route and is unaffected. Feature-level applies (<c>EnsureStorageExistsAsync</c>) neither read nor
/// write the stamp: only the FULL apply computes the full fingerprint.
/// </para>
/// </summary>
internal static class SchemaFingerprint
{
    public const string TableName = "weasel_schema_fingerprint";

    public static string ComputeFingerprint(Migrator migrator, ISchemaObject[] objects)
    {
        var writer = new StringWriter();

        // Sort for a stable hash regardless of feature registration order.
        foreach (var schemaObject in objects.OrderBy(x => x.Identifier.QualifiedName, StringComparer.Ordinal))
        {
            writer.WriteLine(schemaObject.Identifier.QualifiedName);
            schemaObject.WriteCreateStatement(migrator, writer);
        }

        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(writer.ToString()));
        return Convert.ToHexString(bytes);
    }

    /// <summary>
    /// Reads the stored fingerprint. A missing table (fresh database, feature never used) simply
    /// reads as "no stamp".
    /// </summary>
    public static async Task<string?> TryReadAsync(DbConnection conn, string schemaName, CancellationToken ct)
    {
        try
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"select fingerprint from {schemaName}.{TableName} where id = 1";
            return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;
        }
        catch (DbException)
        {
            // Table (or schema) does not exist yet — no stamp, run the full apply.
            return null;
        }
    }

    /// <summary>
    /// Upserts the stamp after a successful full apply. Table creation is attempted first and an
    /// "already exists" failure is swallowed — plain CREATE TABLE keeps this provider-neutral
    /// (not every provider supports IF NOT EXISTS).
    /// </summary>
    public static async Task RecordAsync(DbConnection conn, string schemaName, string fingerprint,
        CancellationToken ct)
    {
        try
        {
            await using var create = conn.CreateCommand();
            create.CommandText =
                $"create table {schemaName}.{TableName} (id int not null primary key, fingerprint varchar(128) not null, applied_at varchar(64) not null)";
            await create.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        catch (DbException)
        {
            // Already exists — fine.
        }

        await using var delete = conn.CreateCommand();
        delete.CommandText = $"delete from {schemaName}.{TableName} where id = 1";
        await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        await using var insert = conn.CreateCommand();
        insert.CommandText =
            $"insert into {schemaName}.{TableName} (id, fingerprint, applied_at) values (1, @fingerprint, @appliedAt)";

        var fingerprintParam = insert.CreateParameter();
        fingerprintParam.ParameterName = "@fingerprint";
        fingerprintParam.Value = fingerprint;
        insert.Parameters.Add(fingerprintParam);

        var appliedAtParam = insert.CreateParameter();
        appliedAtParam.ParameterName = "@appliedAt";
        appliedAtParam.Value = DateTimeOffset.UtcNow.ToString("O");
        insert.Parameters.Add(appliedAtParam);

        await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}
