namespace Weasel.Postgresql.Tables;


public enum PartitionStrategy
{
    /// <summary>
    ///     No partitioning
    /// </summary>
    None,

    /// <summary>
    ///     Postgresql PARTITION BY RANGE semantics
    /// </summary>
    Range,

    /// <summary>
    /// PARTITION BY LIST semantics
    /// </summary>
    List,

    /// <summary>
    /// PARTITION BY HASH semantics
    /// </summary>
    Hash
}


public interface IPartition
{
    PartitionStrategy Strategy { get; }

    string PartitionName { get; }
    void WriteCreateStatement(Table parent);
}

public class ListPartition<T>: IPartition
{
    public ListPartition(string partitionName, params T[] values)
    {
        PartitionName = partitionName;
        Values = values;
    }

    public T[] Values { get; }
    public string PartitionName { get; }

    public void WriteCreateStatement(Table parent)
    {
        throw new NotImplementedException();
    }

    public PartitionStrategy Strategy { get; }
}

public class RangePartition<T>: IPartition
{
    public RangePartition(string partitionName, T from, T to)
    {
        PartitionName = partitionName;
        From = from;
        To = to;
    }

    public T From { get; }
    public T To { get; }

    public void WriteCreateStatement(Table parent)
    {
        throw new NotImplementedException();
    }

    public PartitionStrategy Strategy { get; }

    public string PartitionName { get; }
}
