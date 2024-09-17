using System.Collections.ObjectModel;
using System.Data.Common;
using JasperFx.Core;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.Postgresql.Tables.Partitioning;

public interface IListPartitionManager
{
    IEnumerable<ListPartition> Partitions();
}

public class ManagedListPartitions : FeatureSchemaBase, IFeatureSchemaWithInitialization<NpgsqlConnection>, IListPartitionManager
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

    public async Task AddPartitionToAllTables(PostgresqlDatabase database, string value, string? suffix, CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        await AddPartitionToAllTables(conn, token, value, suffix).ConfigureAwait(false);

        var tables = database
            .AllObjects()
            .OfType<Table>()
            .Where(x => x.Partitioning is ListPartitioning list && list.PartitionManager == this)
            .ToArray();

        foreach (var table in tables)
        {
            await table.MigrateAsync(conn, token).ConfigureAwait(false);
        }

        await conn.CloseAsync().ConfigureAwait(false);
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
                if (await reader.IsDBNullAsync(1, token).ConfigureAwait(false))
                {
                    suffix = (await reader.GetFieldValueAsync<string>(1, token).ConfigureAwait(false)).ToLowerInvariant();
                }

                _partitions[value] = suffix;
            }

            _hasInitialized = true;
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }
}
