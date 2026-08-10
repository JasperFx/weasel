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
    public const string TableName = "weasel_schema_fingerprints";

    /// <summary>
    /// The pre-weasel#439 table: a single row (<c>id = 1</c>) holding one fingerprint. Dropped
    /// best-effort on the first record, because two logical databases sharing a physical database
    /// overwrote each other in it. It is a cache, so discarding it costs one full apply.
    /// </summary>
    private const string LegacyTableName = "weasel_schema_fingerprint";

    /// <summary>
    /// How many stamps to keep. Rows are keyed by fingerprint rather than by any per-database identity
    /// -- there is no identity available here that is reliably distinct between two stores sharing a
    /// physical database -- so a configuration change leaves the old row behind rather than replacing
    /// it, and co-located databases each add their own. Every eviction is self-healing: a database
    /// whose stamp was pruned simply runs one full apply and re-stamps.
    /// </summary>
    private const int MaxStamps = 25;

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
    /// Is this exact configuration already stamped? Asking for one fingerprint rather than reading
    /// "the" fingerprint is what makes co-located databases independent: each looks only for its own
    /// hash and is indifferent to its neighbours' rows. A missing table (fresh database, feature never
    /// used) simply reads as "no stamp".
    /// </summary>
    public static async Task<bool> HasStampAsync(DbConnection conn, string schemaName, string fingerprint,
        CancellationToken ct)
    {
        try
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"select fingerprint from {schemaName}.{TableName} where fingerprint = @fingerprint";

            var parameter = cmd.CreateParameter();
            parameter.ParameterName = "@fingerprint";
            parameter.Value = fingerprint;
            cmd.Parameters.Add(parameter);

            return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) is string;
        }
        catch (DbException)
        {
            // Table (or schema) does not exist yet — no stamp, run the full apply.
            return false;
        }
    }

    /// <summary>
    /// Records the stamp after a successful full apply. Table creation is attempted first and an
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
                $"create table {schemaName}.{TableName} (fingerprint varchar(128) not null primary key, applied_at varchar(64) not null)";
            await create.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        catch (DbException)
        {
            // Already exists — fine.
        }

        await dropLegacyTableAsync(conn, schemaName, ct).ConfigureAwait(false);

        // Delete-then-insert rather than an upsert: the syntax for the latter is not portable, and
        // this runs only on the slow path, immediately after a full apply.
        await using (var delete = conn.CreateCommand())
        {
            delete.CommandText = $"delete from {schemaName}.{TableName} where fingerprint = @fingerprint";
            AddParameter(delete, "@fingerprint", fingerprint);
            await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await using (var insert = conn.CreateCommand())
        {
            insert.CommandText =
                $"insert into {schemaName}.{TableName} (fingerprint, applied_at) values (@fingerprint, @appliedAt)";
            AddParameter(insert, "@fingerprint", fingerprint);
            AddParameter(insert, "@appliedAt", DateTimeOffset.UtcNow.ToString("O"));
            await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await pruneAsync(conn, schemaName, ct).ConfigureAwait(false);
    }

    private static void AddParameter(DbCommand command, string name, string value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    /// <summary>
    /// Removes the single-row table this replaced, so a database upgrading to weasel#439 does not carry
    /// a dead table around forever. Best effort: it is housekeeping, and a caller without DROP rights
    /// should still get a working stamp.
    /// </summary>
    private static async Task dropLegacyTableAsync(DbConnection conn, string schemaName, CancellationToken ct)
    {
        try
        {
            await using var drop = conn.CreateCommand();
            drop.CommandText = $"drop table {schemaName}.{LegacyTableName}";
            await drop.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        catch (DbException)
        {
            // Not there, or not ours to drop.
        }
    }

    /// <summary>
    /// Caps the table at <see cref="MaxStamps" /> rows, oldest first. The ranking is done client-side
    /// against the ISO-8601 timestamps (which sort lexicographically, being fixed-width UTC) because
    /// "delete all but the newest N" has no portable spelling — LIMIT, TOP and FETCH FIRST are all
    /// provider-specific. Best effort for the same reason as the legacy drop: an apply that succeeded
    /// must not be reported as failed because its housekeeping could not run.
    /// </summary>
    private static async Task pruneAsync(DbConnection conn, string schemaName, CancellationToken ct)
    {
        try
        {
            var timestamps = new List<string>();

            await using (var read = conn.CreateCommand())
            {
                read.CommandText = $"select applied_at from {schemaName}.{TableName} order by applied_at desc";
                await using var reader = await read.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    timestamps.Add(reader.GetString(0));
                }
            }

            if (timestamps.Count <= MaxStamps)
            {
                return;
            }

            await using var delete = conn.CreateCommand();
            delete.CommandText = $"delete from {schemaName}.{TableName} where applied_at < @threshold";
            AddParameter(delete, "@threshold", timestamps[MaxStamps - 1]);
            await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        catch (DbException)
        {
            // Housekeeping only — an uncapped table still works correctly.
        }
    }
}
