using System.Data.Common;
using Weasel.Core;

namespace DocSamples;

#region sample_ISchemaObject_interface
public interface ISchemaObject_Sample
{
    DbObjectName Identifier { get; }

    void WriteCreateStatement(Migrator migrator, TextWriter writer);
    void WriteDropStatement(Migrator rules, TextWriter writer);
    void ConfigureQueryCommand(Weasel.Core.DbCommandBuilder builder);
    Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default);
    IEnumerable<DbObjectName> AllNames();
}
#endregion

#region sample_ISchemaObjectDelta_interface
public interface ISchemaObjectDelta_Sample
{
    ISchemaObject SchemaObject { get; }
    SchemaPatchDifference Difference { get; }
    void WriteUpdate(Migrator rules, TextWriter writer);
    void WriteRollback(Migrator rules, TextWriter writer);
    void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer);
}
#endregion

#region sample_SchemaObjectDelta_base_class
public abstract class SchemaObjectDelta_Sample<T> : ISchemaObjectDelta where T : ISchemaObject
{
    public T Expected { get; } = default!;
    public T? Actual { get; }
    public SchemaPatchDifference Difference { get; }

    public ISchemaObject SchemaObject => Expected;

    protected abstract SchemaPatchDifference compare(T expected, T? actual);
    public abstract void WriteUpdate(Migrator rules, TextWriter writer);
    public abstract void WriteRollback(Migrator rules, TextWriter writer);
    public abstract void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer);
}
#endregion

#region sample_ISchemaObjectWithPostProcessing_interface
public interface ISchemaObjectWithPostProcessing_Sample : ISchemaObject
{
    void PostProcess(ISchemaObject[] allObjects);
}
#endregion
