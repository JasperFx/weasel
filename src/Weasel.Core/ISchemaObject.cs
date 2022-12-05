using System.Data.Common;
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
///     Responsible for the desired configuration of a single database object like
///     a table, sequence, of function.
/// </summary>
public interface ISchemaObject
{
    /// <summary>
    ///     Name of this database object
    /// </summary>
    DbObjectName Identifier { get; }

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
    Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader);

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

    public SchemaPatchDifference Difference { get; }
    public abstract void WriteUpdate(Migrator rules, TextWriter writer);

    public virtual void WriteRollback(Migrator rules, TextWriter writer)
    {
        Expected.WriteDropStatement(rules, writer);
        Actual!.WriteCreateStatement(rules, writer);
    }

    public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
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
