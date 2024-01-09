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

    public void Apply(ICommandBuilder builder)
    {
        var parameters = builder.AppendWithParameters(_sql);
        for (var i = 0; i < parameters.Length; i++)
        {
            parameters[i].Value = _parameters[i].Value;
            if (_parameters[i].DbType.HasValue)
            {
                parameters[i].NpgsqlDbType = _parameters[i].DbType.Value;
            }
        }
    }
}
