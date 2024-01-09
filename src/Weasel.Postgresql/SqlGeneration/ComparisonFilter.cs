namespace Weasel.Postgresql.SqlGeneration;

public class ComparisonFilter: IReversibleWhereFragment
{
    /// <summary>
    ///     Used for NOT operator conversions
    /// </summary>
    public static readonly IDictionary<string, string> NotOperators = new Dictionary<string, string>
    {
        { "=", "!=" },
        { "!=", "=" },
        { ">", "<=" },
        { ">=", "<" },
        { "<", ">=" },
        { "<=", ">" }
    };

    /// <summary>
    ///     Used when reordering a Binary comparison
    /// </summary>
    public static readonly IDictionary<string, string> OppositeOperators = new Dictionary<string, string>
    {
        { "=", "=" },
        { "!=", "!=" },
        { ">", "<" },
        { ">=", "<=" },
        { "<", ">" },
        { "<=", ">=" }
    };

    public ComparisonFilter(ISqlFragment left, ISqlFragment right, string op)
    {
        Left = left;
        Right = right;
        Op = op;
    }

    public ISqlFragment Left { get; }

    public ISqlFragment Right { get; }

    public string Op { get; private set; }

    public void Apply(ICommandBuilder builder)
    {
        Left.Apply(builder);
        builder.Append(" ");
        builder.Append(Op);
        builder.Append(" ");
        Right.Apply(builder);
    }

    public ISqlFragment Reverse()
    {
        Op = NotOperators[Op];
        return this;
    }
}
