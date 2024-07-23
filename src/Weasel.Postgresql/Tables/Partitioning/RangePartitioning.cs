using JasperFx.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

public class RangePartitioning: IPartitionStrategy
{
    private readonly List<RangePartition> _partitions = new();

    public IReadOnlyList<RangePartition> Partitions => _partitions;

    /// <summary>
    /// The database columns to use as part of the hashing strategy
    /// </summary>
    public string[] Columns { get; init; }

    public void WritePartitionBy(TextWriter writer)
    {
        writer.WriteLine($") PARTITION BY RANGE ({Columns.Join(", ")});");
    }

    public RangePartitioning AddRange<T>(string suffix, T from, T to)
    {
        var partition = new RangePartition(suffix, from.FormatSqlValue(), to.FormatSqlValue());
        _partitions.Add(partition);

        return this;
    }

    public void WriteCreateStatement(TextWriter writer, Table parent)
    {
        foreach (var partition in _partitions)
        {
            partition.WriteCreateStatement(writer, parent);
            writer.WriteLine();
        }

        writer.WriteDefaultPartition(parent.Identifier);
    }
}
