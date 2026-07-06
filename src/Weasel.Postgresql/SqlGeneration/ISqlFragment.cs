namespace Weasel.Postgresql.SqlGeneration;

public interface ISqlFragment: Weasel.Core.SqlGeneration.ISqlFragment
{
    void Apply(ICommandBuilder builder);

    /// <summary>
    ///     Satisfies the dialect-neutral <see cref="Weasel.Core.SqlGeneration.ISqlFragment" /> contract by
    ///     forwarding to the PostgreSQL-typed <see cref="Apply(ICommandBuilder)" /> overload. Provided as a
    ///     default interface method so existing implementers only need to implement the dialect overload.
    /// </summary>
    void Weasel.Core.SqlGeneration.ISqlFragment.Apply(Weasel.Core.ICommandBuilder builder)
        => Apply((ICommandBuilder)builder);
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
    ///     PostgreSQL-typed <see cref="Children" />; the covariance of <see cref="IEnumerable{T}" /> makes the
    ///     dialect children a valid neutral enumerable.
    /// </summary>
    IEnumerable<Weasel.Core.SqlGeneration.ISqlFragment> Weasel.Core.SqlGeneration.ICompoundFragment.Children
        => Children;
}
