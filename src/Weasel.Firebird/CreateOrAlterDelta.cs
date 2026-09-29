using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Firebird;

/// <summary>
///     The delta for a Firebird object that updates itself with <c>CREATE OR ALTER</c>: a view, a
///     function, a stored procedure or a trigger.
/// </summary>
/// <remarks>
///     <para>
///         The update is the create statement on its own, never a drop followed by a create.
///         <c>CREATE OR ALTER</c> changes the object in place, so the views, procedures and triggers
///         that depend on it, and the privileges granted on it, survive the change; a drop would be
///         refused while anything depends on the object, and would take the grants with it.
///     </para>
///     <para>
///         The delta keeps the object read out of the catalog, so a rollback puts that definition back
///         rather than leaving the change in place.
///     </para>
/// </remarks>
public class CreateOrAlterDelta: ISchemaObjectDelta
{
    private readonly bool _isRemoved;

    public CreateOrAlterDelta(ISchemaObject expected, ISchemaObject? actual, SchemaPatchDifference difference,
        bool isRemoved = false, IReadOnlyList<string>? differences = null)
    {
        SchemaObject = expected ?? throw new ArgumentNullException(nameof(expected));
        Actual = actual;
        Difference = difference;
        _isRemoved = isRemoved;
        Differences = differences ?? [];
    }

    public ISchemaObject SchemaObject { get; }

    /// <summary>
    ///     The object as the catalog describes it, or null when it does not exist.
    /// </summary>
    public ISchemaObject? Actual { get; }

    public SchemaPatchDifference Difference { get; }

    /// <summary>
    ///     What differs between the model and the catalog, for an update: which parameter, the body, the
    ///     table a trigger fires on.
    /// </summary>
    public IReadOnlyList<string> Differences { get; }

    public void WriteUpdate(Migrator rules, TextWriter writer)
    {
        if (_isRemoved)
        {
            (Actual ?? SchemaObject).WriteDropStatement(rules, writer);
            return;
        }

        SchemaObject.WriteCreateStatement(rules, writer);
    }

    public void WriteRollback(Migrator rules, TextWriter writer)
    {
        if (Actual == null)
        {
            SchemaObject.WriteDropStatement(rules, writer);
            return;
        }

        Actual.WriteCreateStatement(rules, writer);
    }

    public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
    {
        Actual?.WriteCreateStatement(rules, writer);
    }

    public override string ToString()
        => Differences.Count == 0
            ? $"{SchemaObject.Identifier.QualifiedName} {Difference}"
            : $"{SchemaObject.Identifier.QualifiedName} {Difference}: {Differences.Join("; ")}";
}
