using System.Linq.Expressions;

namespace Weasel.Postgresql.SqlGeneration;

public class WhereInArrayFilter: ISqlFragment
{
    private readonly string _locator;
    private readonly CommandParameter _values;

    public WhereInArrayFilter(string locator, ConstantExpression values)
    {
        _locator = locator;
        _values = new CommandParameter(values);
    }

    public void Apply(IPostgresqlCommandBuilder builder)
    {
        builder.Append(_locator);
        builder.Append(" = ANY(:");
        var parameter = _values.AddParameter(builder);
        builder.Append(parameter.ParameterName);
        builder.Append(")");
    }

}
