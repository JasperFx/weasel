namespace Weasel.SqlServer.SqlGeneration;

public interface ISqlFragment: Weasel.Core.SqlGeneration.ISqlFragment
{
    void Apply(CommandBuilder builder);

    bool Contains(string sqlText);

    /// <summary>
    ///     Satisfies the dialect-neutral <see cref="Weasel.Core.SqlGeneration.ISqlFragment" /> contract by
    ///     forwarding to the SQL Server <see cref="Apply(CommandBuilder)" /> overload. Provided as a default
    ///     interface method so existing implementers only need to implement the dialect overload.
    ///     <para>
    ///     SQL Server fragments are applied against the concrete <see cref="CommandBuilder" />; a neutral
    ///     consumer must therefore supply a <see cref="CommandBuilder" /> (not a <c>BatchBuilder</c>) — the
    ///     same constraint that already applied to the dialect <see cref="Apply(CommandBuilder)" /> overload.
    ///     </para>
    /// </summary>
    void Weasel.Core.SqlGeneration.ISqlFragment.Apply(Weasel.Core.ICommandBuilder builder)
        => Apply((CommandBuilder)builder);
}

/// <summary>
/// Model a Sql fragment that may hold one or more other fragements
/// Used to search through a tree of fragments
/// </summary>
public interface ICompoundFragment: Weasel.Core.SqlGeneration.ICompoundFragment, ISqlFragment
{
    new IEnumerable<ISqlFragment> Children { get; }

    /// <summary>
    ///     Bridges the neutral <see cref="Weasel.Core.SqlGeneration.ICompoundFragment.Children" /> to the
    ///     SQL Server-typed <see cref="Children" />; the covariance of <see cref="IEnumerable{T}" /> makes the
    ///     dialect children a valid neutral enumerable.
    /// </summary>
    IEnumerable<Weasel.Core.SqlGeneration.ISqlFragment> Weasel.Core.SqlGeneration.ICompoundFragment.Children
        => Children;
}
