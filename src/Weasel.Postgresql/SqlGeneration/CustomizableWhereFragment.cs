using JasperFx.Core;

namespace Weasel.Postgresql.SqlGeneration;

public class CustomizableWhereFragment: ISqlFragment
{
    private readonly CommandParameter[] _parameters;
    private readonly string _sql;
    private readonly string _token;

    public CustomizableWhereFragment(string sql, string paramReplacementToken, params CommandParameter[] parameters)
    {
        _sql = sql;
        _parameters = parameters;
        _token = paramReplacementToken;
    }

    public void Apply(CommandBuilder builder)
    {
        // TODO -- reevaluate this code. Use the new AppendWithParameters maybe?
        var sql = _sql;

        foreach (var def in _parameters)
        {
            var param = def.AddParameter(builder);
            sql = sql.ReplaceFirst(_token, ":" + param.ParameterName);
        }

        builder.Append(sql);
    }

    public bool Contains(string sqlText)
    {
        return _sql.Contains(sqlText);
    }
}
