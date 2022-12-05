namespace Weasel.SqlServer.SqlGeneration;

public class NotWhereFragment: ISqlFragment, IWhereFragmentHolder
{
    private readonly IWhereFragmentHolder _parent;

    public NotWhereFragment(IWhereFragmentHolder parent)
    {
        _parent = parent;
    }

    public ISqlFragment Inner { get; set; } = null!;

    public void Apply(CommandBuilder builder)
    {
        builder.Append("NOT(");
        Inner.Apply(builder);
        builder.Append(')');
    }

    public bool Contains(string sqlText)
    {
        return "NOT".Contains(sqlText) || Inner.Contains(sqlText);
    }

    void IWhereFragmentHolder.Register(ISqlFragment fragment)
    {
        if (fragment is IReversibleWhereFragment r)
        {
            _parent.Register(r.Reverse());
        }
        else
        {
            Inner = fragment;
            _parent.Register(this);
        }
    }
}
