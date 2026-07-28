using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql;
using Weasel.Postgresql.Tables;

namespace Weasel.Postgresql.Tables.Partitioning;


public interface IListPartitionManager
{
    IEnumerable<ListPartition> Partitions();
}

public class ManagedListPartitions : FeatureSchemaBase, IDatabaseInitializer<NpgsqlConnection>, IListPartitionManager
{
    private readonly Table _table;
    private Dictionary<string, string> _partitions = new();
    private bool _hasInitialized;
    private readonly SemaphoreSlim _semaphoreSlim = new(1);

    public ManagedListPartitions(string identifier, DbObjectName tableName): base(identifier, new PostgresqlMigrator())
    {
        _table = new Table(tableName);
        _table.AddColumn<string>("partition_value").AsPrimaryKey().NotNull();

        // if null, use the tenant id.ToLowerInvariant() as the partition name
        _table.AddColumn<string>("partition_suffix");
    }

    public ReadOnlyDictionary<string, string> Partitions => new(_partitions);

    protected override IEnumerable<ISchemaObject> schemaObjects()
    {
        yield return _table;
    }

    IEnumerable<ListPartition> IListPartitionManager.Partitions()
    {
        var paths = _partitions.GroupBy(x => x.Value);
        foreach (var path in paths)
        {
            yield return new ListPartition(path.Key, path.Select(x => $"'{x.Key}'").ToArray());
        }
    }

    /// <summary>
    /// Utility to overwrite all data in the managed partition table
    /// </summary>
    /// <param name="database"></param>
    /// <param name="values">Pairs of "value": "partition name"</param>
    public async Task ResetValues(PostgresqlDatabase database, Dictionary<string, string> values, CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        await _table.MigrateAsync(conn, token).ConfigureAwait(false);

        var tx = await conn.BeginTransactionAsync(token).ConfigureAwait(false);

        await conn.CreateCommand($"delete from {_table.Identifier}")
            .ExecuteNonQueryAsync(token)
            .ConfigureAwait(false);

        foreach (var pair in values)
        {
            await tx
                .CreateCommand(
                    $"insert into {_table.Identifier} (partition_value, partition_suffix) values (:value, :suffix) on conflict (partition_value) do update set partition_suffix = :suffix")
                .With("value", pair.Key)
                .With("suffix", pair.Value ?? pair.Key).ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        _partitions = values;

        await tx.CommitAsync(token).ConfigureAwait(false);
        await conn.CloseAsync().ConfigureAwait(false);
    }

    public async Task DropPartitionFromAllTablesForValue(PostgresqlDatabase database, ILogger logger, string value,
        CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        // This is idempotent, so just do it here
        await InitializeAsync(conn, token).ConfigureAwait(false);

        if (!_partitions.TryGetValue(value, out var suffix))
        {
            await conn.CloseAsync().ConfigureAwait(false);
            throw new ArgumentOutOfRangeException(nameof(value),
                $"Could not find a partition with the value '{value}'");
        }

        // weasel#391: this used to resolve the value to its suffix and then drop BY SUFFIX, which deletes
        // every registry row in the bucket and DROPs the shared partition table. For a bucketed suffix that
        // silently destroyed unrelated co-tenants' rows. When other values still share the partition, narrow
        // the bucket instead; the partition is only released once its last member is gone.
        var sanitizedSuffix = ListPartition.SanitizeSuffix(suffix);
        var survivors = _partitions
            .Where(pair => pair.Key != value && ListPartition.SanitizeSuffix(pair.Value) == sanitizedSuffix)
            .Select(pair => pair.Key)
            .ToArray();

        if (survivors.Length == 0)
        {
            await conn.CloseAsync().ConfigureAwait(false);
            await DropPartitionFromAllTables(database, logger, [suffix], token).ConfigureAwait(false);
            return;
        }

        await removeValueFromSharedPartitionAsync(database, logger, conn, value, sanitizedSuffix, survivors,
            token).ConfigureAwait(false);

        await conn.CloseAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Remove one value from a shared ("bucketed") partition without disturbing the other values in it:
    /// delete just this value's rows, then re-bind the partition to the remaining members.
    /// </summary>
    private async Task removeValueFromSharedPartitionAsync(PostgresqlDatabase database, ILogger logger,
        NpgsqlConnection conn, string value, string sanitizedSuffix, string[] survivors, CancellationToken token)
    {
        await conn.CreateCommand($"delete from {_table.Identifier} where partition_value = :value")
            .With("value", value)
            .ExecuteNonQueryAsync(token).ConfigureAwait(false);

        _partitions.Remove(value);

        var remainingValues = survivors.Select(x => x.FormatSqlValue()).ToArray();

        var tables = database
            .AllObjects()
            .OfType<Table>()
            .Where(x => x.Partitioning is ListPartitioning list && list.PartitionManager == this)
            .ToArray();

        foreach (var table in tables)
        {
            var partitionName = PostgresqlObjectName.From(
                new DbObjectName(table.Identifier.Schema, table.Identifier.Name + "_" + sanitizedSuffix));

            var bound = await fetchPartitionBoundAsync(conn, table.Identifier.Schema,
                table.Identifier.Name + "_" + sanitizedSuffix, token).ConfigureAwait(false);

            // Same reasoning as the by-suffix drop: a configured parent may never have been physically
            // migrated onto this database, in which case there is genuinely nothing to narrow.
            if (bound == null)
            {
                logger.LogInformation(
                    "Skipped narrowing partition {Partition} because it is not physically present on this database",
                    partitionName);
                continue;
            }

            var column = ((ListPartitioning)table.Partitioning!).Columns[0];

            // ATTACH re-validates every remaining row against the narrower bound, so the departing value's
            // rows have to go first. PostgreSQL partition removal is inherently data-removing — the
            // whole-bucket path DROPs the table outright — so this is the same contract at value granularity.
            await conn.CreateCommand($"delete from {partitionName} where {column} = :value")
                .With("value", value)
                .ExecuteNonQueryAsync(token).ConfigureAwait(false);

            await rebindPartitionAsync(conn, table, partitionName, bound.IsAttached, remainingValues, token)
                .ConfigureAwait(false);

            logger.LogInformation(
                "Removed value {Value} from shared partition {Partition}; {Count} value(s) remain",
                value, partitionName, survivors.Length);
        }
    }

    public async Task DropPartitionFromAllTables(PostgresqlDatabase database, ILogger logger, string[] suffixNames, CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        // This is idempotent, so just do it here
        await InitializeAsync(conn, token).ConfigureAwait(false);

        var cmd = conn
            .CreateCommand(
                $"delete from {_table.Identifier} where partition_suffix = ANY(:suffix)")
            .With("suffix", suffixNames);

        await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);

        logger.LogInformation("Removed partition suffixes {SuffixNames} from partition list {TableName}", suffixNames.Join(", "), _table.Identifier.QualifiedName);

        var tables = database
            .AllObjects()
            .OfType<Table>()
            .Where(x => x.Partitioning is ListPartitioning list && list.PartitionManager == this)
            .ToArray();

        // var ordered = tables.TopologicalSort(t =>
        //     t.ForeignKeys.Select(fk => tables.FirstOrDefault(x => Equals(x.Identifier, fk.LinkedTable))).Where(x => x != null)
        //         .ToArray()!).Reverse().Intersect(tables).ToArray();


        foreach (var table in tables)
        {
            // weasel#344: a configured partition-parent table may never have been physically migrated onto
            // this database (e.g. a registered tenant whose doc/event tables don't exist on this shard yet).
            // In that case the DETACH below throws 42P01 (relation does not exist) and propagates, even
            // though there is genuinely nothing to drop. Skip tables that aren't physically present — the
            // child partition cannot exist without its parent, so the subsequent DROP would be a no-op too.
            var parentExists = await conn
                .CreateCommand("select to_regclass(:qualified) is not null")
                .With("qualified", table.Identifier.QualifiedName)
                .ExecuteScalarAsync(token).ConfigureAwait(false);

            if (parentExists is not true)
            {
                logger.LogInformation(
                    "Skipped dropping partitions for table {tableName} because it is not physically present on this database",
                    table.Identifier);
                continue;
            }

            foreach (var suffixName in suffixNames)
            {
                // weasel#338: the CREATE path (ListPartition) names the partition table with a SANITIZED
                // suffix, so the DROP path must sanitize identically or it targets a different, invalid
                // table name — an unquoted '-' from a hyphenated/GUID tenant id yields 42601, and even
                // where quoting kicks in it resolves "<parent>_<raw>" instead of the "<parent>_<sanitized>"
                // that create actually produced. The managed-partition metadata table above keys on the
                // raw suffix (matching what was stored), only the physical DDL name is normalized.
                var sanitizedSuffix = ListPartition.SanitizeSuffix(suffixName);
                var partitionName = PostgresqlObjectName.From(
                    new DbObjectName(table.Identifier.Schema, table.Identifier.Name + "_" + sanitizedSuffix));

                try
                {
                    var command = $"alter table {table.Identifier.QualifiedName} detach partition {partitionName} CONCURRENTLY;";
                    await conn.CreateCommand(
                            command)
                        .ExecuteNonQueryAsync(token).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    var command = $"alter table {table.Identifier.QualifiedName} detach partition {partitionName};";
                    await conn.CreateCommand(
                            command)
                        .ExecuteNonQueryAsync(token).ConfigureAwait(false);
                }

                await conn.CreateCommand(
                        $"drop table if exists {partitionName} cascade;")
                    .ExecuteNonQueryAsync(token).ConfigureAwait(false);

                logger.LogInformation("Executed script to drop table partition with suffix {suffixName} for table {tableName}", suffixName, table.Identifier);
            }
        }


    }

    public async Task AddPartitionToAllTables(PostgresqlDatabase database, string value, string? suffix, CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        // This is idempotent, so just do it here
        await InitializeAsync(conn, token).ConfigureAwait(false);

        await AddPartitionToAllTables(conn, token, value, suffix).ConfigureAwait(false);

        var resolvedSuffix = suffix.IsEmpty() ? value.ToLowerInvariant() : suffix!;
        await additivelyMigrateTablesForNewPartitions(NullLogger.Instance, database, token, conn,
            new[] { new KeyValuePair<string, string>(value, resolvedSuffix) }).ConfigureAwait(false);

        await conn.CloseAsync().ConfigureAwait(false);
    }

    public async Task<TablePartitionStatus[]> AddPartitionToAllTables(ILogger logger, PostgresqlDatabase database, Dictionary<string, string> values, CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        // This is idempotent, so just do it here
        await InitializeAsync(conn, token).ConfigureAwait(false);

        await _table.MigrateAsync(conn, token).ConfigureAwait(false);

        await using var tx = await conn.BeginTransactionAsync(token).ConfigureAwait(false);
        foreach (var pair in values)
        {
            var cmd = tx
                .CreateCommand(
                    $"insert into {_table.Identifier} (partition_value, partition_suffix) values (:value, :suffix) on conflict (partition_value) do update set partition_suffix = :suffix")
                .With("value", pair.Key)
                .With("suffix", pair.Value);

            _partitions[pair.Key] = pair.Value;

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        await tx.CommitAsync(token).ConfigureAwait(false);

        var list = await additivelyMigrateTablesForNewPartitions(logger, database, token, conn, values.ToArray()).ConfigureAwait(false);

        await conn.CloseAsync().ConfigureAwait(false);

        return list.ToArray();
    }

    private async Task<List<TablePartitionStatus>> additivelyMigrateTablesForNewPartitions(ILogger logger,
        PostgresqlDatabase database, CancellationToken token, NpgsqlConnection conn,
        IReadOnlyCollection<KeyValuePair<string, string>> newPartitions)
    {
        var list = new List<TablePartitionStatus>();
        if (newPartitions.Count == 0) return list;

        var tables = database
            .AllObjects()
            .OfType<Table>()
            .Where(x => x.Partitioning is ListPartitioning list && list.PartitionManager == this)
            .ToArray();

        // Targeted, purely-additive creation: create ONLY the partitions for the values registered in THIS
        // call, each as CREATE TABLE IF NOT EXISTS (idempotent, via ListPartition.WriteCreateStatement).
        //
        // It deliberately does NOT reconcile every table against the manager's full partition set. That
        // full-set reconcile is wrong once these managed-partition tables live in sharded databases: the
        // manager's in-memory partition set is the UNION of tenant values seen across ALL databases, so
        // reconciling each table against it (a) created partitions for tenants that live on OTHER shard
        // databases (over-provisioning — every shard accumulated every tenant's partition), and (b) tripped
        // ListPartitioning.CreateDelta's "the actual table has partitions the desired set does not" guard
        // into PartitionDelta.Rebuild, which then skipped adding the genuinely-missing partition altogether
        // (under-provisioning — a tenant's own partition was never created, so its first write failed with
        // 23514 "no partition of relation ... found for row"). Creating exactly what was requested avoids
        // both, and needs no IgnorePartitionsInMigration toggling because it never runs the generic table
        // delta on the shared per-mapping Table instance (see JasperFx/marten#4706, #4713).
        var buckets = resolveBuckets(newPartitions);

        foreach (var table in tables)
        {
            try
            {
                foreach (var bucket in buckets)
                {
                    await createOrWidenPartitionAsync(conn, table, bucket.Key, bucket.Value, token)
                        .ConfigureAwait(false);
                }

                list.Add(new TablePartitionStatus(table.Identifier, PartitionMigrationStatus.Complete));
            }
            catch (Exception e)
            {
                logger.LogError(e, "Error trying to add table partitions to {Table}", table.Identifier);
                list.Add(new TablePartitionStatus(table.Identifier, PartitionMigrationStatus.Failed));
            }
        }

        return list;
    }

    /// <summary>
    /// weasel#391: several values may deliberately share ONE suffix — tenant "bucketing", where many small
    /// tenants live in a single physical partition to stay clear of per-table partition ceilings. The partition
    /// table is named for the SANITIZED suffix, so that is the real bucket key, and its bound has to carry
    /// EVERY value in the bucket. The previous code emitted one single-value <see cref="ListPartition"/> per
    /// value, so the second member of a bucket was swallowed by CREATE TABLE IF NOT EXISTS and its writes
    /// then failed with 23514 "no partition of relation found for row".
    /// </summary>
    private Dictionary<string, string[]> resolveBuckets(
        IReadOnlyCollection<KeyValuePair<string, string>> newPartitions)
    {
        var buckets = new Dictionary<string, string[]>();

        foreach (var group in newPartitions.GroupBy(pair => ListPartition.SanitizeSuffix(pair.Value)))
        {
            // The desired bound is the bucket's FULL membership, not merely the values supplied in THIS
            // call. Bucket members are normally registered one tenant at a time, so widening an existing
            // partition has to keep the members already in it. _partitions is authoritative here: callers
            // record the new pairs into it before reaching this method.
            buckets[group.Key] = _partitions
                .Where(pair => ListPartition.SanitizeSuffix(pair.Value) == group.Key)
                .Select(pair => pair.Key)
                .Concat(group.Select(pair => pair.Key))
                .Distinct()
                .Select(value => value.FormatSqlValue())
                .ToArray();
        }

        return buckets;
    }

    private static async Task createOrWidenPartitionAsync(NpgsqlConnection conn, Table table,
        string sanitizedSuffix, string[] desiredValues, CancellationToken token)
    {
        var partitionName = PostgresqlObjectName.From(
            new DbObjectName(table.Identifier.Schema, table.Identifier.Name + "_" + sanitizedSuffix));

        var bound = await fetchPartitionBoundAsync(conn, table.Identifier.Schema,
            table.Identifier.Name + "_" + sanitizedSuffix, token).ConfigureAwait(false);

        if (bound == null)
        {
            // Brand new partition — one CREATE carrying the whole bucket.
            var writer = new StringWriter();
            var partition = new ListPartition(sanitizedSuffix, desiredValues);
            ((IPartition)partition).WriteCreateStatement(writer, table);

            await conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync(token).ConfigureAwait(false);
            return;
        }

        // Purely additive: this path never narrows a bound (drop owns that), so the new bound is the union
        // of what is already there and what was asked for.
        var covered = bound.Values.Select(x => x.NormalizePartitionValue()).ToHashSet();
        var missing = desiredValues.Where(x => !covered.Contains(x.NormalizePartitionValue())).ToArray();

        if (missing.Length == 0 && bound.IsAttached)
        {
            return;
        }

        await rebindPartitionAsync(conn, table, partitionName, bound.IsAttached,
            bound.Values.Concat(missing).ToArray(), token).ConfigureAwait(false);
    }

    /// <summary>
    /// The current partition bound of <paramref name="partitionTableName"/>, or null when no such table
    /// exists. A table that exists but is not attached to a parent comes back with an empty, unattached
    /// bound so the caller can ATTACH it without first issuing a DETACH that would fail.
    /// </summary>
    private static async Task<PartitionBound?> fetchPartitionBoundAsync(NpgsqlConnection conn,
        string schemaName, string partitionTableName, CancellationToken token)
    {
        await using var reader = await conn
            .CreateCommand(
                "select pg_get_expr(c.relpartbound, c.oid) from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = :schema and c.relname = :name")
            .With("schema", schemaName)
            .With("name", partitionTableName)
            .ExecuteReaderAsync(token).ConfigureAwait(false);

        if (!await reader.ReadAsync(token).ConfigureAwait(false))
        {
            return null;
        }

        if (await reader.IsDBNullAsync(0, token).ConfigureAwait(false))
        {
            return new PartitionBound([], false);
        }

        var expression = await reader.GetFieldValueAsync<string>(0, token).ConfigureAwait(false);
        return new PartitionBound(expression.GetStringWithinParantheses().ToDelimitedArray(','), true);
    }

    /// <summary>
    /// CREATE TABLE IF NOT EXISTS cannot alter the bound of an existing partition, so changing a bucket's
    /// membership is DETACH followed by re-ATTACH. Both run inside ONE transaction: a failure between them
    /// would leave the partition orphaned and every tenant in the bucket unroutable.
    /// </summary>
    private static async Task rebindPartitionAsync(NpgsqlConnection conn, Table table,
        PostgresqlObjectName partitionName, bool isAttached, string[] values, CancellationToken token)
    {
        await using var tx = await conn.BeginTransactionAsync(token).ConfigureAwait(false);

        if (isAttached)
        {
            await tx.CreateCommand(
                    $"alter table {table.Identifier.QualifiedName} detach partition {partitionName};")
                .ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        await tx.CreateCommand(
                $"alter table {table.Identifier.QualifiedName} attach partition {partitionName} for values in ({values.Join(", ")});")
            .ExecuteNonQueryAsync(token).ConfigureAwait(false);

        await tx.CommitAsync(token).ConfigureAwait(false);
    }

    private sealed record PartitionBound(string[] Values, bool IsAttached);

    public Task AddPartitionToAllTables(NpgsqlConnection conn, CancellationToken token, string value, string? suffix = null)
    {
        if (value.IsEmpty()) throw new ArgumentNullException(nameof(value));

        if (suffix.IsEmpty()) suffix = value.ToLowerInvariant();

        var cmd = conn
            .CreateCommand(
                $"insert into {_table.Identifier} (partition_value, partition_suffix) values (:value, :suffix) on conflict (partition_value) do update set partition_suffix = :suffix")
            .With("value", value)
            .With("suffix", suffix);

        _partitions[value] = suffix;

        return cmd.ExecuteNonQueryAsync(token);
    }

    public void ForceReload()
    {
        _hasInitialized = false;
    }

    public async Task InitializeAsync(PostgresqlDatabase database, CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        await InitializeAsync(conn, token).ConfigureAwait(false);

        await conn.CloseAsync().ConfigureAwait(false);
    }

    public async Task InitializeAsync(NpgsqlConnection conn, CancellationToken token)
    {
        if (_hasInitialized) return;
        await _semaphoreSlim.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_hasInitialized) return;

            _partitions.Clear();
            await using var reader = await conn
                .CreateCommand($"select partition_value, partition_suffix from {_table.Identifier.QualifiedName}")
                .ExecuteReaderAsync(token).ConfigureAwait(false);

            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                var value = await reader.GetFieldValueAsync<string>(0, token).ConfigureAwait(false);
                var suffix = value.ToLowerInvariant();
                if (!(await reader.IsDBNullAsync(1, token).ConfigureAwait(false)))
                {
                    suffix =
                        (await reader.GetFieldValueAsync<string>(1, token).ConfigureAwait(false)).ToLowerInvariant();
                }

                _partitions[value] = suffix;
            }

            _hasInitialized = true;
        }
        // 42P01: relation "public.mt_tenant_partitions" does not exist
        catch (NpgsqlException e)
        {
            if (e.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                _hasInitialized = true;
                return;
            }

            throw;
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }
}

public enum PartitionMigrationStatus
{
    Complete,
    Failed,
    RequiresTableRebuild
}

public record TablePartitionStatus(DbObjectName Identifier, PartitionMigrationStatus Status);

