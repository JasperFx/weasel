using System.Linq.Expressions;
using Npgsql;
using NpgsqlTypes;

namespace Weasel.Postgresql.SqlGeneration;

public class CommandParameter: ISqlFragment
{
    public CommandParameter()
    {
    }

    public CommandParameter(ConstantExpression expression)
    {
        Value = expression.Value;
        DbType = PostgresqlProvider.Instance.ToParameterType(expression.Type == typeof(object)
            ? expression.Value!.GetType()
            : expression.Type);
    }

    public CommandParameter(object? value)
    {
        Value = value;
        if (value != null)
        {
            DbType = PostgresqlProvider.Instance.TryGetDbType(value.GetType())!.Value;
        }
    }

    public CommandParameter(object value, NpgsqlDbType npgsqlDbType)
    {
        Value = value;
        DbType = npgsqlDbType;
    }

    public object? Value { get; }
    public NpgsqlDbType DbType { get; }

    public void Apply(CommandBuilder builder)
    {
        builder.AppendParameter(Value, DbType);
    }

    public bool Contains(string sqlText)
    {
        return false;
    }

    public NpgsqlParameter AddParameter(CommandBuilder builder)
    {
        return builder.AddParameter(Value, DbType);
    }
}
