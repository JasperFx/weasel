namespace Weasel.Postgresql.Tables
{
    public interface IPartition
    {
        void WriteCreateStatement(Table parent);
        PartitionStrategy Strategy { get; }
        
        string PartitionName { get; }
    }

    public class ListPartition<T> : IPartition
    {
        public string PartitionName { get; }
        public T[] Values { get; }

        public ListPartition(string partitionName, params T[] values)
        {
            PartitionName = partitionName;
            Values = values;
        }

        public void WriteCreateStatement(Table parent)
        {
            throw new System.NotImplementedException();
        }

        public PartitionStrategy Strategy { get; }
    }
    
    public class RangePartition<T> : IPartition
    {
        public RangePartition(string partitionName, T @from, T to)
        {
            PartitionName = partitionName;
            From = @from;
            To = to;
        }

        public void WriteCreateStatement(Table parent)
        {
            throw new System.NotImplementedException();
        }

        public PartitionStrategy Strategy { get; }

        public string PartitionName { get; }
        public T From { get; }
        public T To { get; }
    }
}