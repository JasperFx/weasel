using Weasel.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

public interface IPartitionStrategy
{
    void WriteCreateStatement(TextWriter writer, Table parent);
    string[] Columns { get; }

    void WritePartitionBy(TextWriter writer);

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
