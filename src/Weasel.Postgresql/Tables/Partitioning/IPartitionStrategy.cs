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
}

public enum PartitionDelta
{
    None,
    Additive,
    Rebuild
}

public class RePartitionDelta : ISchemaObjectDelta
{
    private readonly Table _table;
    private readonly Table _actual;

    public RePartitionDelta(Table expected, Table actual)
    {
        _table = expected;
        _actual = actual;
    }

    public ISchemaObject SchemaObject => _table;
    public SchemaPatchDifference Difference => SchemaPatchDifference.Invalid;
    public void WriteUpdate(Migrator rules, TextWriter writer)
    {
        throw new NotImplementedException();
    }

    public void WriteRollback(Migrator rules, TextWriter writer)
    {
        throw new NotImplementedException();
    }

    public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
    {
        throw new NotImplementedException();
    }
}
