namespace Weasel.Postgresql.SqlGeneration;

public class NotWhereFragment: ISqlFragment, IWhereFragmentHolder
{
    private readonly IWhereFragmentHolder _parent;

    public NotWhereFragment(IWhereFragmentHolder parent)
    {
        _parent = parent;
    }

    public ISqlFragment Inner { get; set; } = null!;

    public void Apply(IPostgresqlCommandBuilder builder)
    {
        builder.Append("NOT(");
        Inner.Apply(builder);
        builder.Append(')');
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
