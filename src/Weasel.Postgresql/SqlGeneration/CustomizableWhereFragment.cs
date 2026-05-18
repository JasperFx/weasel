using System.Collections;
using System.Diagnostics.CodeAnalysis;
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

    /// <summary>
    ///     Calls <see cref="ICommandBuilder.AddParameters(object)" /> when the first parameter
    ///     looks like an anonymous type or <c>IDictionary</c> with <c>string</c> keys. That
    ///     overload reflects over the object's public properties — annotated upstream with
    ///     <see cref="RequiresUnreferencedCodeAttribute" />. <see cref="ISqlFragment" /> has
    ///     wide internal implementation, so propagating <c>[RequiresUnreferencedCode]</c> here
    ///     would force every implementor to declare it too (IL2046 cascade). Instead we
    ///     <see cref="UnconditionalSuppressMessageAttribute">suppress</see> the IL2026
    ///     diagnostic at the two call sites below with a Justification — AOT-trim-clean
    ///     consumers should construct this fragment with explicit
    ///     <see cref="CommandParameter" /> entries (the array-of-parameters path, not the
    ///     anonymous-type path). weasel#263.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "ICommandBuilder.AddParameters(object) is only reached on the anonymous-type / IDictionary input shape; AOT consumers pass explicit CommandParameter[] entries which take the lower branch. weasel#263.")]
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
