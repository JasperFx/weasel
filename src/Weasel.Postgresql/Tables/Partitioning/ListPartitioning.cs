using System.Data.Common;
using System.Diagnostics;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

public class ListPartitioning: IPartitionStrategy
{
    private readonly List<ListPartition> _partitions = new();
    public string[] Columns { get; init; }

    public IReadOnlyList<ListPartition> Partitions => _partitions;

    /// <summary>
    /// </summary>
    public bool EnableDefaultPartition { get; set; } = true;

    public IListPartitionManager? PartitionManager { get; private set; }

    /// <summary>
    /// Apply a list partition manager that will control the exact partitions
    /// </summary>
    /// <param name="strategy"></param>
    /// <returns></returns>
    public ListPartitioning UsePartitionManager(IListPartitionManager strategy)
    {
        EnableDefaultPartition = false;
        PartitionManager = strategy;

        return this;
    }

    /// <summary>
    /// Add another list partition table based on the supplied table suffix and values
    /// </summary>
    /// <param name="suffix"></param>
    /// <param name="values"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public ListPartitioning AddPartition<T>(string suffix, params T[] values)
    {
        var partition = new ListPartition(suffix, values.Select(x => x.FormatSqlValue()).ToArray());
        _partitions.Add(partition);

        return this;
    }

    void IPartitionStrategy.WriteCreateStatement(TextWriter writer, Table parent)
    {
        var partitions = PartitionManager?.Partitions() ?? _partitions;

        foreach (IPartition partition in partitions)
        {
            partition.WriteCreateStatement(writer, parent);
            writer.WriteLine();
        }

        if (EnableDefaultPartition)
        {
            writer.WriteDefaultPartition(parent.Identifier);
        }
    }

    void IPartitionStrategy.WritePartitionBy(TextWriter writer)
    {
        writer.WriteLine($") PARTITION BY LIST ({Columns.Join(", ")});");
    }

    PartitionDelta IPartitionStrategy.CreateDelta(Table parent, IPartitionStrategy actual, out IPartition[] missing)
    {
        missing = default;
        if (actual is ListPartitioning other)
        {
            var partitions = PartitionManager?.Partitions().ToList() ?? _partitions;

            if (!Columns.SequenceEqual(other.Columns))
            {
                return PartitionDelta.Rebuild;
            }

            if (parent.IgnorePartitionsInMigration) return PartitionDelta.None;

            var match = partitions.OrderBy(x => x.Suffix).ToArray()
                .SequenceEqual(other.Partitions.OrderBy(x => x.Suffix).ToArray());

            if (match) return PartitionDelta.None;

            // We've already done a SequenceEqual, so we know the counts aren't the same
            // and if there are more actual partitions than expected, we need to do a rebalance
            if (other.Partitions.Count > partitions.Count) return PartitionDelta.Rebuild;

            // If any partitions are in the actual that are no longer expected, that's an automatic rebuild
            if (other._partitions.Any(x => !partitions.Contains(x))) return PartitionDelta.Rebuild;

            missing = partitions.Where(x => !other._partitions.Contains(x)).OfType<IPartition>().ToArray();
            return missing.Any() ? PartitionDelta.Additive : PartitionDelta.Rebuild;
        }
        else
        {
            return PartitionDelta.Rebuild;
        }
    }

    public IEnumerable<string> PartitionTableNames(Table parent)
    {
        foreach (var partition in _partitions)
        {
            yield return $"{parent.Identifier.Name.ToLowerInvariant()}_{partition.Suffix.ToLowerInvariant()}";
        }

        if (EnableDefaultPartition)
        {
            yield return $"{parent.Identifier.Name.ToLowerInvariant()}_default";
        }
    }

    public async Task ReadPartitionsAsync(DbObjectName identifier, DbDataReader reader, CancellationToken ct)
    {
        var expectedDefaultName = identifier.Name + "_default";
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var partitionName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            var expression = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);

            if (partitionName == expectedDefaultName)
            {
                HasExistingDefault = true;
            }
            else
            {
                var partition = ListPartition.Parse(identifier, partitionName, expression);
                _partitions.Add(partition);
            }
        }
    }

    public bool HasExistingDefault { get; private set; }

    /// <summary>
    /// Disable the default partition
    /// </summary>
    /// <returns></returns>
    public ListPartitioning DisableDefaultPartition()
    {
        EnableDefaultPartition = false;
        return this;
    }
}
