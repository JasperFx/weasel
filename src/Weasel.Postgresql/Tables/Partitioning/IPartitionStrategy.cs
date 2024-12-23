using Weasel.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

public interface IPartitionStrategy
{
    void WriteCreateStatement(TextWriter writer, Table parent);

    /// <summary>
    /// The table columns from the parent table that are part of the partitioning
    /// </summary>
    string[] Columns { get; }

    /// <summary>
    /// Writes out the partitioning clause within the table's creation SQL
    /// </summary>
    /// <param name="writer"></param>
    void WritePartitionBy(TextWriter writer);

    /// <summary>
    /// Used by Weasel to detect any differences between the partitioning as defined in the Weasel Table model
    /// and the actual partitions that exist in the actual database
    /// </summary>
    /// <param name="parent"></param>
    /// <param name="actual"></param>
    /// <param name="missing"></param>
    /// <returns></returns>
    PartitionDelta CreateDelta(Table parent, IPartitionStrategy actual, out IPartition[] missing);

    IEnumerable<string> PartitionTableNames(Table parent);
}

public enum PartitionDelta
{
    None,
    Additive,
    Rebuild
}


