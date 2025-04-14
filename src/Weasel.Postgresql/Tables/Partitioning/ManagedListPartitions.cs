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

        await conn.CloseAsync().ConfigureAwait(false);

        try
        {
            var suffix = _partitions.Single(x => x.Value == value).Key;
            await DropPartitionFromAllTables(database, logger, [suffix], token).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            throw new ArgumentOutOfRangeException(nameof(value),
                $"Could not find a partition with the value '{value}'");
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
            foreach (var suffixName in suffixNames)
            {
                var partitionName = $"{table.Identifier}_{suffixName}";

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

        await additivelyMigrateTablesForNewPartitions(NullLogger.Instance, database, token, conn).ConfigureAwait(false);

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

        var list = await additivelyMigrateTablesForNewPartitions(logger, database, token, conn).ConfigureAwait(false);

        await conn.CloseAsync().ConfigureAwait(false);

        return list.ToArray();
    }

    private async Task<List<TablePartitionStatus>> additivelyMigrateTablesForNewPartitions(ILogger logger, PostgresqlDatabase database,
        CancellationToken token, NpgsqlConnection conn)
    {
        var tables = database
            .AllObjects()
            .OfType<Table>()
            .Where(x => x.Partitioning is ListPartitioning list && list.PartitionManager == this)
            .ToArray();

        var list = new List<TablePartitionStatus>();

        foreach (var table in tables)
        {
            var existing = await table.FetchExistingAsync(conn, token).ConfigureAwait(false);
            var delta = new TableDelta(table, existing);
            if (delta.PartitionDelta == PartitionDelta.Additive)
            {
                try
                {
                    await table.MigrateAsync(conn, token).ConfigureAwait(false);
                    foreach (var partition in delta.MissingPartitions)
                    {
                        logger.LogInformation("Added partition {Partition} to table {TableName}", partition, table.Identifier);
                    }

                    list.Add(new TablePartitionStatus(table.Identifier, PartitionMigrationStatus.Complete));
                }
                catch (Exception e)
                {
                    logger.LogError(e, "Error trying to add table partitions to {Table}", table.Identifier);
                    list.Add(new TablePartitionStatus(table.Identifier, PartitionMigrationStatus.Failed));
                }
            }
            else if (delta.PartitionDelta == PartitionDelta.None)
            {
                list.Add(new TablePartitionStatus(table.Identifier, PartitionMigrationStatus.Complete));
            }
            else
            {
                list.Add(new TablePartitionStatus(table.Identifier, PartitionMigrationStatus.RequiresTableRebuild));
            }
        }

        return list;
    }

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
        _hasInitialized = true;
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
        if (_hasInitialized) return;

        _partitions.Clear();

        try
        {
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

