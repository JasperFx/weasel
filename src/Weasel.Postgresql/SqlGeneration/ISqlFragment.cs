namespace Weasel.Postgresql.SqlGeneration;

public interface ISqlFragment
{
    void Apply(ICommandBuilder builder);
}

/// <summary>
/// Model a Sql fragment that may hold one or more other fragements
/// Used to search through a tree of fragments
/// </summary>
public interface ICompoundFragment: ISqlFragment
{
    IEnumerable<ISqlFragment> Children { get; }
}

