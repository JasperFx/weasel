using System.Collections;
using JasperFx.Core.Reflection;

namespace Weasel.Postgresql.SqlGeneration;

public class CustomizableWhereFragment: ISqlFragment
{
    private readonly object[] _parameters;
    private readonly string _sql;
    private readonly char _token;

    public CustomizableWhereFragment(string sql, string paramReplacementToken, params object[] parameters)
    {
        _sql = sql;
        _parameters = parameters;
        _token = paramReplacementToken.ToCharArray()[0];
    }

    public void Apply(ICommandBuilder builder)
    {
        // backwards compatibility, old version of this class accepted CommandParameter[]
        if (_parameters is [CommandParameter { Value: { } firstVal }] && (firstVal.IsAnonymousType() || firstVal is IDictionary { Keys: ICollection<string> }))
        {
            builder.Append(_sql);
            builder.AddParameters(firstVal);
            return;
        }

        if (_parameters is [{ } first] && (first.IsAnonymousType() || first is IDictionary { Keys: ICollection<string> }))
        {
            builder.Append(_sql);
            builder.AddParameters(first);
            return;
        }
        

        var parameters = builder.AppendWithParameters(_sql, _token);
        for (var i = 0; i < parameters.Length; i++)
        {
            // backwards compatibility, old version of this class accepted CommandParameter[]
            var commandParameter = _parameters[i] as CommandParameter ?? new CommandParameter(_parameters[i]);
            parameters[i].Value = commandParameter.Value;
            if (commandParameter.DbType.HasValue)
            {
                parameters[i].NpgsqlDbType = commandParameter.DbType.Value;
            }
        }
    }
}
