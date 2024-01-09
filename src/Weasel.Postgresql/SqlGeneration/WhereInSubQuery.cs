namespace Weasel.Postgresql.SqlGeneration;

// TODO -- move to Weasel
public class WhereInSubQuery: ISqlFragment
{
    private readonly string _tableName;

    public WhereInSubQuery(string tableName)
    {
        _tableName = tableName;
    }

    public void Apply(ICommandBuilder builder)
    {
        builder.Append("id in (select id from ");
        builder.Append(_tableName);
        builder.Append(")");
    }

}
