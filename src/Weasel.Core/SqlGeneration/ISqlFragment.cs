namespace Weasel.Core.SqlGeneration;

/// <summary>
///     Dialect-neutral SQL fragment. Applies itself to the shared, non-generic
///     <see cref="ICommandBuilder" /> so a database-agnostic consumer can compose WHERE
///     fragments, select clauses, and commands without referencing a specific Weasel provider.
///     <para>
///     Each provider's own <c>ISqlFragment</c> derives from this and keeps its dialect-typed
///     <c>Apply</c> overload; the neutral <see cref="Apply" /> requirement is satisfied on the
///     dialect interface via a default interface method that forwards to the dialect overload.
///     </para>
/// </summary>
public interface ISqlFragment
{
    void Apply(ICommandBuilder builder);
}

/// <summary>
///     Models a SQL fragment that may hold one or more other fragments.
///     Used to search through a tree of fragments in a dialect-neutral way.
/// </summary>
public interface ICompoundFragment: ISqlFragment
{
    IEnumerable<ISqlFragment> Children { get; }
}
