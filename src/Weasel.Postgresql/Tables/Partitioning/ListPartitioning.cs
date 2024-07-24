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
        foreach (IPartition partition in _partitions)
        {
            partition.WriteCreateStatement(writer, parent);
            writer.WriteLine();
        }

        writer.WriteDefaultPartition(parent.Identifier);
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
            if (!Columns.SequenceEqual(other.Columns))
            {
                return PartitionDelta.Rebuild;
            }

            if (parent.IgnorePartitionsInMigration) return PartitionDelta.None;

            var match = _partitions.OrderBy(x => x.Suffix).ToArray()
                .SequenceEqual(other.Partitions.OrderBy(x => x.Suffix).ToArray());

            if (match) return PartitionDelta.None;

            // We've already done a SequenceEqual, so we know the counts aren't the same
            // and if there are more actual partitions than expected, we need to do a rebalance
            if (other.Partitions.Count > Partitions.Count) return PartitionDelta.Rebuild;

            // If any partitions are in the actual that are no longer expected, that's an automatic rebuild
            if (other._partitions.Any(x => !_partitions.Contains(x))) return PartitionDelta.Rebuild;

            missing = _partitions.Where(x => !other._partitions.Contains(x)).OfType<IPartition>().ToArray();
            return missing.Any() ? PartitionDelta.Additive : PartitionDelta.Rebuild;

        }
        else
        {
            return PartitionDelta.Rebuild;
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
}
