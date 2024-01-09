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
        Value = value ?? DBNull.Value;
        if (value == null) return;

        var valueType = value.GetType();

        var dbType = PostgresqlProvider.Instance.TryGetDbType(valueType);

        if (!dbType.HasValue)
            return;

        DbType = dbType.Value;
    }

    public CommandParameter(object value, NpgsqlDbType npgsqlDbType)
    {
        Value = value ?? DBNull.Value;
        DbType = npgsqlDbType;
    }

    public object? Value { get; }
    public NpgsqlDbType? DbType { get; set; }

    public void Apply(ICommandBuilder builder)
    {
        builder.AppendParameter(Value, DbType);
    }

    public NpgsqlParameter AddParameter(ICommandBuilder builder)
    {
        return builder.AppendParameter(Value, DbType);
    }
}
