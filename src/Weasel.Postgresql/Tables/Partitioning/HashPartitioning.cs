using System.Data.Common;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

public class HashPartitioning : IPartitionStrategy
{
    /// <summary>
    /// The database columns to use as part of the hashing strategy
    /// </summary>
    public string[] Columns { get; init; }

    /// <summary>
    /// The suffix names for the partitioned table names. The modulo/remainder values
    /// will be created automatically based on the number of suffixes
    /// </summary>
    public string[] Suffixes
    {
        get
        {
            return _partitions.Select(x => x.Suffix).ToArray();
        }
        set
        {
            _partitions.Clear();
            var modulus = value.Length;
            var remainder = 0;
            foreach (var suffix in value)
            {
                var partition = new HashPartition(suffix, modulus, remainder);
                _partitions.Add(partition);
                remainder++;
            }
        }
    }

    private readonly List<HashPartition> _partitions = new();

    public IReadOnlyList<HashPartition> Partitions => _partitions;

    public void WriteCreateStatement(TextWriter writer, Table parent)
    {
        foreach (var partition in _partitions)
        {
            partition.WriteCreateStatement(writer, parent);
            writer.WriteLine();
        }
    }

    public void WritePartitionBy(TextWriter writer)
    {
        writer.WriteLine($") PARTITION BY HASH ({Columns.Join(", ")});");
    }

    public PartitionDelta CreateDelta(Table parent, IPartitionStrategy actual, out IPartition[] missing)
    {
        missing = default;
        if (actual is HashPartitioning other)
        {
            if (!Columns.SequenceEqual(other.Columns))
            {
                return PartitionDelta.Rebuild;
            }

            var match = _partitions.OrderBy(x => x.Modulus).ToArray()
                .SequenceEqual(other.Partitions.OrderBy(x => x.Modulus).ToArray());

            if (match) return PartitionDelta.None;

            return PartitionDelta.Rebuild;
        }
        else
        {
            return PartitionDelta.Rebuild;
        }
    }

    public IEnumerable<string> PartitionTableNames(Table parent)
    {
        foreach (var suffix in Suffixes)
        {
            yield return $"{parent.Identifier.Name.ToLowerInvariant()}_{suffix}";
        }
    }

    public static async Task<HashPartitioning> ReadPartitionsAsync(DbObjectName identifier, List<string> columns,
        DbDataReader reader, CancellationToken ct)
    {
        var partitioning = new HashPartitioning { Columns = columns.ToArray() };

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var partitionName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            var expression = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);

            var suffix = identifier.GetSuffixName(partitionName);
            var partition = HashPartition.Parse(suffix, expression);

            partitioning._partitions.Add(partition);
        }

        return partitioning;
    }
}
