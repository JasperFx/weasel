using Weasel.Core;

namespace Weasel.Oracle;

/// <summary>
///     The delta for an Oracle schema object that updates itself by being re-created, never by
///     being dropped first.
/// </summary>
/// <remarks>
///     <para>
///         Six object types had written this class out longhand — views, triggers, stored
///         procedures, packages, synonyms and functions — because the default
///         <see cref="SchemaObjectDelta" /> emits a DROP ahead of the CREATE and on Oracle that
///         combination cannot be executed.
///     </para>
///     <para>
///         The reason is worth stating exactly, because "one statement per delta" is the shorthand
///         and it is not quite right. Oracle has no <c>DROP … IF EXISTS</c> before 23c, so every
///         drop in this provider is an anonymous PL/SQL block that swallows the
///         "does not exist" error. <c>OracleMigrator</c> executes one command per statement,
///         splitting on the <c>/</c> terminator — and a drop block written immediately before a
///         <c>CREATE OR REPLACE</c>, with no <c>/</c> between them, arrives at ODP.NET as a PL/SQL
///         block followed by a DDL statement in a single command. That is
///         <c>PLS-00103: Encountered the symbol "CREATE"</c>.
///     </para>
///     <para>
///         The drop is not needed anyway: every one of these object types is created with
///         <c>CREATE OR REPLACE</c>, which is idempotent. So the update is the create statement on
///         its own. An object marked removed writes its drop instead — alone, which is fine,
///         because a lone PL/SQL block is a perfectly good command.
///     </para>
///     <para>
///         A create that spans more than one statement is still fine, as long as it separates them
///         itself: <c>Packages.Package</c> emits the specification, a <c>/</c>, and then the body,
///         and the migrator splits those into two commands. What matters is that nothing is
///         prefixed to a statement that did not ask for it.
///     </para>
///     <para>
///         <c>Tables.TableDelta</c> deliberately does not use this. A table's update is a sequence
///         of ALTERs with no single create to fall back on, and it manages its own statement
///         separators.
///     </para>
/// </remarks>
public class OracleReplaceDelta: ISchemaObjectDelta
{
    private readonly bool _isRemoved;

    public OracleReplaceDelta(ISchemaObject schemaObject, SchemaPatchDifference difference, bool isRemoved = false)
    {
        SchemaObject = schemaObject ?? throw new ArgumentNullException(nameof(schemaObject));
        Difference = difference;
        _isRemoved = isRemoved;
    }

    public ISchemaObject SchemaObject { get; }

    public SchemaPatchDifference Difference { get; }

    public void WriteUpdate(Migrator rules, TextWriter writer)
        => WriteReplacement(SchemaObject, _isRemoved, rules, writer);

    /// <summary>
    ///     The rule itself, exposed so a delta that needs
    ///     <see cref="SchemaObjectDelta{T}" />'s expected/actual comparison can apply it too
    ///     rather than keeping a second copy — <c>Functions.FunctionDelta</c> is the one that does.
    /// </summary>
    public static void WriteReplacement(
        ISchemaObject schemaObject, bool isRemoved, Migrator rules, TextWriter writer)
    {
        if (isRemoved)
        {
            schemaObject.WriteDropStatement(rules, writer);
            return;
        }

        schemaObject.WriteCreateStatement(rules, writer);
    }

    /// <summary>
    ///     Nothing. Rolling back to a previous definition would mean having kept it, and these
    ///     deltas carry only the expected object — the shape every one of the six hand-written
    ///     versions had.
    /// </summary>
    public void WriteRollback(Migrator rules, TextWriter writer)
    {
    }

    public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
        => throw new NotSupportedException();

    public override string ToString() => $"{SchemaObject.Identifier.QualifiedName} {Difference}";
}
