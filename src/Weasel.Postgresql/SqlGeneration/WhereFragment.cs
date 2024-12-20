namespace Weasel.Postgresql.SqlGeneration;

public class WhereFragment: CustomizableWhereFragment
{
    public WhereFragment(string sql, params object[] parameters): base(sql, "?", parameters)
    {
    }
}

public class LiteralFalse: WhereFragment, IReversibleWhereFragment
{
    public LiteralFalse() : base("FALSE")
    {
    }

    public ISqlFragment Reverse()
    {
        return new LiteralTrue();
    }
}

public class LiteralTrue: WhereFragment, IReversibleWhereFragment
{
    public LiteralTrue() : base("TRUE")
    {
    }

    public ISqlFragment Reverse()
    {
        return new LiteralFalse();
    }
}
