using JasperFx.Core;

namespace Weasel.Postgresql.Tables;

public partial class Table
{
    public void PartitionBy(PartitionStrategy strategy, params string[] columns)
    {
        var matching = Columns.Where(x => columns.Any(c => c.EqualsIgnoreCase(x.Name)))
            .Where(x => !x.IsPrimaryKey).Select(x => x.Name).ToArray();
        if (matching.Any())
        {
            throw new ArgumentOutOfRangeException("columns",
                $"Columns {matching.Join(", ")} must be primary key columns to be used as the target of a partition");
        }

        PartitionStrategy = strategy;
        PartitionExpressions.Clear();
        PartitionExpressions.AddRange(columns);
    }

    public void PartitionByRange(params string[] columnOrExpressions)
    {
        PartitionStrategy = PartitionStrategy.Range;
        PartitionExpressions.Clear();
        PartitionExpressions.AddRange(columnOrExpressions);
    }

    public void PartitionByList(params string[] columnOrExpressions)
    {
        PartitionStrategy = PartitionStrategy.List;
        PartitionExpressions.Clear();
        PartitionExpressions.AddRange(columnOrExpressions);
    }

    public void PartitionByHash(HashPartitioning partitioning)
    {
        PartitionStrategy = PartitionStrategy.Hash;
        PartitionExpressions.Clear();
        PartitionExpressions.AddRange(partitioning.Columns);
        _partitions.AddRange(partitioning.BuildPartitions());
    }

    public void ClearPartitions()
    {
        PartitionStrategy = PartitionStrategy.None;
        PartitionExpressions.Clear();
    }


    public IList<string> PartitionExpressions { get; } = new List<string>();

    /// <summary>
    ///     PARTITION strategy for this table
    /// </summary>
    public PartitionStrategy PartitionStrategy { get; private set; } = PartitionStrategy.None;


    public void AddListPartition<T>(string suffix, params T[] values)
    {
        var partition = new ListPartition<T>(suffix, values);
        _partitions.Add(partition);
    }

    private readonly List<IPartition> _partitions = new();

    public IReadOnlyList<IPartition> Partitions => _partitions;

    public void AddRangePartition<T>(string suffix, T from, T to)
    {
        var partition = new RangePartition<T>(suffix, from, to);
        _partitions.Add(partition);
    }
}
