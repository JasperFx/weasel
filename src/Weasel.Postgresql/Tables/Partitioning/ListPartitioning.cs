using JasperFx.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

public class ListPartitioning: IPartitionStrategy
{
    private readonly List<ListPartition> _partitions = new();
    public string[] Columns { get; init; }

    public IReadOnlyList<ListPartition> Partitions => _partitions;

    public ListPartitioning AddPartition<T>(string suffix, params T[] values)
    {
        var partition = new ListPartition(suffix, values.Select(x => x.FormatSqlValue()).ToArray());
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

    public void WritePartitionBy(TextWriter writer)
    {
        writer.WriteLine($") PARTITION BY LIST ({Columns.Join(", ")});");
    }
}
