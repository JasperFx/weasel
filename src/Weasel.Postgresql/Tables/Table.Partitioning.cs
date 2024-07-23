using JasperFx.Core;
using Weasel.Postgresql.Tables.Partitioning;

namespace Weasel.Postgresql.Tables;

public partial class Table
{
    public IPartitionStrategy? Partitioning { get; private set; }

    public RangePartitioning PartitionByRange(params string[] columnOrExpressions)
    {
        var partitioning = new RangePartitioning { Columns = columnOrExpressions };
        Partitioning = partitioning;
        return partitioning;
    }

    public ListPartitioning PartitionByList(params string[] columnOrExpressions)
    {
        var partitioning = new ListPartitioning { Columns = columnOrExpressions };
        Partitioning = partitioning;
        return partitioning;
    }

    public void PartitionByHash(HashPartitioning partitioning)
    {
        Partitioning = partitioning;
    }

}
