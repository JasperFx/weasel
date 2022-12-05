namespace Weasel.Postgresql.Tables;

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
