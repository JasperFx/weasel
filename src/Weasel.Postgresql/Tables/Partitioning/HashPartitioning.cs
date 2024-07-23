using JasperFx.Core;

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
    public string[] Suffixes { get; init; }

    // TODO -- make the partition building be lazy next time?
    public void WriteCreateStatement(TextWriter writer, Table parent)
    {
        foreach (var partition in BuildPartitions())
        {
            partition.WriteCreateStatement(writer, parent);
            writer.WriteLine();
        }
    }

    internal IEnumerable<HashPartition> BuildPartitions()
    {
        var modulus = Suffixes.Length;
        var remainder = 0;
        foreach (var suffix in Suffixes)
        {
            yield return new HashPartition(suffix, modulus, remainder);
            remainder++;
        }
    }

    public void WritePartitionBy(TextWriter writer)
    {
        writer.WriteLine($") PARTITION BY HASH ({Columns.Join(", ")});");
    }
}
