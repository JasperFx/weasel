using System.Data.Common;
using JasperFx;
using JasperFx.Core;

namespace Weasel.Core;

public class SchemaMigrationException: Exception
{
    public SchemaMigrationException(string? message): base(message)
    {
    }

    public SchemaMigrationException(string? message, Exception? innerException): base(message, innerException)
    {
    }

    public SchemaMigrationException(AutoCreate autoCreate, IEnumerable<object> invalids): base(
        $"Cannot derive schema migrations for {invalids.Select(x => x.ToString()!).Join(", ")} AutoCreate.{autoCreate}")
    {
    }
}

/// <summary>
/// Schema objects that may need to analyze other schema objects in order to correctly
/// generate their own model. Originally introduced for PostgreSQL partitioning with foreign keys
/// </summary>
public interface ISchemaObjectWithPostProcessing : ISchemaObject
{
    void PostProcess(ISchemaObject[] allObjects);
}

/// <summary>
///     Schema objects that write identifiers into their DDL which are not themselves named
///     database objects — a table's column names, its primary key constraint name, its check
///     constraint names. <see cref="ISchemaObject.AllNames" /> cannot carry these: it yields
///     <see cref="DbObjectName" />, and callers read the schema off every name it returns
///     (<c>SchemaMigration.Schemas</c>, <c>DatabaseBase.ApplyAllConfiguredChangesToDatabaseAsync</c>),
///     so a column name would arrive there claiming to be an object in a schema.
/// </summary>
/// <remarks>
///     The migration path validates these alongside <see cref="ISchemaObject.AllNames" />
///     (weasel#448). Before that, a table's identifier, index names and foreign key names were
///     checked and everything else went straight into the DDL unexamined.
/// </remarks>
/// <summary>
///     A delta that reports <see cref="SchemaPatchDifference.Invalid" /> because the change cannot
///     be made in place, but which knows how to make it anyway without losing the data.
/// </summary>
/// <remarks>
///     <para>
///         <c>Invalid</c> means "I cannot express this as an ALTER", and the default answer to it
///         is to drop the object and create it again. For a table that answer throws away every row
///         — which is right when there is no alternative, and catastrophic when there is.
///     </para>
///     <para>
///         SQLite is where there is one. It cannot change a column's type, add a foreign key or
///         change a primary key with <c>ALTER TABLE</c>, so every such change is <c>Invalid</c> —
///         and its <c>TableDelta</c> has always known how to do it properly: create a new table,
///         copy the surviving columns across, drop the old one, rename, and put the indexes and
///         triggers back. Nothing called it, so a column type change silently emptied the table
///         (weasel#477).
///     </para>
///     <para>
///         Implementing this does not make the change safe enough to apply under
///         <c>AutoCreate.CreateOrUpdate</c>. It is still destructive in the sense that matters for
///         permission — a column being removed takes its data with it — so it still requires
///         <c>AutoCreate.All</c>. What changes is only what <c>All</c> then does.
///     </para>
/// </remarks>
public interface ISchemaObjectDeltaWithRebuild : ISchemaObjectDelta
{
    /// <summary>
    ///     Whether <see cref="ISchemaObjectDelta.WriteUpdate" /> can carry out this change without
    ///     discarding the object's data. When false the caller falls back to drop-and-create.
    /// </summary>
    bool CanRebuildInPlace { get; }
}

public interface ISchemaObjectWithLocalIdentifiers : ISchemaObject
{
    /// <summary>
    ///     Every identifier this object writes into DDL that is not one of its
    ///     <see cref="ISchemaObject.AllNames" />. Duplicates are fine; the caller only validates.
    /// </summary>
    IEnumerable<string> LocalIdentifiers();
}

/// <summary>
///     Responsible for the desired configuration of a single database object like
///     a table, sequence, of function.
/// </summary>
public interface ISchemaObject
{
    /// <summary>
    ///     Name of this database object
    /// </summary>
    public DbObjectName Identifier { get; }

    /// <summary>
    ///     Write the SQL statement(s) to create this object in a database
    /// </summary>
    /// <param name="migrator"></param>
    /// <param name="writer"></param>
    void WriteCreateStatement(Migrator migrator, TextWriter writer);

    /// <summary>
    ///     Write the SQL statement(s) to drop this object from a database
    /// </summary>
    /// <param name="rules"></param>
    /// <param name="writer"></param>
    void WriteDropStatement(Migrator rules, TextWriter writer);

    /// <summary>
    ///     Register the necessary queries to check the existing state of this schema
    ///     object in the database
    /// </summary>
    /// <param name="builder"></param>
    void ConfigureQueryCommand(DbCommandBuilder builder);

    /// <summary>
    ///     Given the results of the query built by ConfigureQueryCommand(), return an
    ///     object describing the difference between the as configured object and the object
    ///     in the database
    /// </summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default);

    /// <summary>
    ///     Returns all the database object names created by this ISchemaObject. Most of the
    ///     time this is just Identifier, but tables may create named indexes that would also be
    ///     reflected here
    /// </summary>
    /// <returns></returns>
    IEnumerable<DbObjectName> AllNames();
}

public abstract class SchemaObjectDelta<T>: ISchemaObjectDelta where T : ISchemaObject
{
    protected SchemaObjectDelta(T expected, T? actual)
    {
        if (expected == null)
        {
            throw new ArgumentNullException(nameof(expected));
        }

        Expected = expected;
        Actual = actual;

        Difference = compare(Expected, Actual);
    }

    public T Expected { get; }
    public T? Actual { get; }

    public ISchemaObject SchemaObject => Expected;

    public SchemaPatchDifference Difference { get; protected set; }
    public abstract void WriteUpdate(Migrator rules, TextWriter writer);

    public virtual void WriteRollback(Migrator rules, TextWriter writer)
    {
        Expected.WriteDropStatement(rules, writer);
        Actual!.WriteCreateStatement(rules, writer);
    }

    public virtual void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
    {
        Actual!.WriteCreateStatement(rules, writer);
    }

    protected abstract SchemaPatchDifference compare(T expected, T? actual);
}

public class SchemaObjectDelta: ISchemaObjectDelta
{
    public SchemaObjectDelta(ISchemaObject schemaObject, SchemaPatchDifference difference)
    {
        SchemaObject = schemaObject;
        Difference = difference;
    }

    public ISchemaObject SchemaObject { get; }
    public SchemaPatchDifference Difference { get; }

    public void WriteUpdate(Migrator rules, TextWriter writer)
    {
        SchemaObject.WriteDropStatement(rules, writer);
        SchemaObject.WriteCreateStatement(rules, writer);
    }

    public void WriteRollback(Migrator rules, TextWriter writer)
    {
    }

    public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
    {
        throw new NotSupportedException();
    }
}
