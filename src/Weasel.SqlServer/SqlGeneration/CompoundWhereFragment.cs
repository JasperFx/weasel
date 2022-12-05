using JasperFx.Core;

namespace Weasel.SqlServer.SqlGeneration;

public class CompoundWhereFragment: ISqlFragment, IWhereFragmentHolder
{
    private readonly IList<ISqlFragment> _children = new List<ISqlFragment>();

    private CompoundWhereFragment(string separator, params ISqlFragment[] children)
    {
        Separator = separator;
        _children.AddRange(children);
    }

    public string Separator { get; }

    public IEnumerable<ISqlFragment> Children => _children;

    public void Apply(CommandBuilder builder)
    {
        if (!_children.Any())
        {
            return;
        }

        var separator = $" {Separator} ";

        builder.Append("(");
        _children[0].Apply(builder);
        for (var i = 1; i < _children.Count; i++)
        {
            builder.Append(separator);
            _children[i].Apply(builder);
        }

        builder.Append(")");
    }

    public bool Contains(string sqlText)
    {
        return _children.Any(x => x.Contains(sqlText));
    }

    public void Register(ISqlFragment fragment)
    {
        _children.Add(fragment);
    }

    public static CompoundWhereFragment And(params ISqlFragment[] children)
    {
        return new CompoundWhereFragment("and ", children);
    }

    public static CompoundWhereFragment And(IEnumerable<ISqlFragment> children)
    {
        return new CompoundWhereFragment(" and ", children.ToArray());
    }

    public static CompoundWhereFragment Or(params ISqlFragment[] children)
    {
        return new CompoundWhereFragment(" or ", children);
    }

    public void Add(ISqlFragment child)
    {
        _children.Add(child);
    }

    public void Remove(ISqlFragment fragment)
    {
        _children.Remove(fragment);
    }

    public void Add(IReadOnlyList<ISqlFragment> extras)
    {
        _children.AddRange(extras);
    }

    public static CompoundWhereFragment For(string separator)
    {
        return new CompoundWhereFragment(separator);
    }
}
