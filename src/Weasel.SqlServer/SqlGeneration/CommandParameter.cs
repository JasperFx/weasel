using System.Data;
using System.Data.SqlClient;
using System.Linq.Expressions;

namespace Weasel.SqlServer.SqlGeneration
{
    public class CommandParameter : ISqlFragment
    {
        public CommandParameter()
        {
        }

        public CommandParameter(ConstantExpression expression)
        {
            Value = expression.Value;
            DbType = SqlServerProvider.Instance.ToParameterType(expression.Type == typeof(object)
                ? expression.Value.GetType()
                : expression.Type);
        }

        public CommandParameter(object value)
        {
            Value = value;
            if (value != null)
            {
                DbType = SqlServerProvider.Instance.TryGetDbType(value.GetType()).Value;
            }
        }

        public CommandParameter(object value, SqlDbType SqlDbType)
        {
            Value = value;
            DbType = SqlDbType;
        }

        public object Value { get; }
        public SqlDbType DbType { get; }

        public void Apply(CommandBuilder builder)
        {
            builder.AppendParameter(Value, DbType);
        }

        public bool Contains(string sqlText)
        {
            return false;
        }

        public SqlParameter AddParameter(CommandBuilder builder)
        {
            return builder.AddParameter(Value, DbType);
        }
    }
}