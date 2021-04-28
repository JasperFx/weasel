using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

#nullable enable

namespace Weasel.Postgresql
{
    public class CommandBuilder: IDisposable
    {
        private readonly NpgsqlCommand _command;

        // TEMP -- will shift this to being pooled later
        private readonly StringBuilder _sql = new StringBuilder();

        public CommandBuilder() : this(new NpgsqlCommand())
        {
        }

        public CommandBuilder(NpgsqlCommand command)
        {
            _command = command;
        }

        public NpgsqlCommand Compile()
        {
            _command.CommandText = _sql.ToString();
            return _command;
        }

        public void Dispose()
        {
        }

        public static NpgsqlCommand BuildCommand(Action<CommandBuilder> configure)
        {
            var cmd = new NpgsqlCommand();
            using (var builder = new CommandBuilder(cmd))
            {
                configure(builder);

                cmd.CommandText = builder.ToString();
            }

            return cmd;
        }

        public void Append(string text)
        {
            _sql.Append(text);
        }

        public void Append(object o)
        {
            _sql.Append(o);
        }

        public override string ToString()
        {
            return _sql.ToString();
        }

        public void Clear()
        {
            _sql.Clear();
        }

        public void AddParameters(object parameters)
        {
            _command.AddParameters(parameters);
        }

        public NpgsqlParameter AddParameter(object value, NpgsqlDbType? dbType = null)
        {
            return _command.AddParameter(value, dbType);
        }

        public void AddJsonParameter(string json)
        {
            _command.AddParameter(json, NpgsqlDbType.Jsonb);
        }

        public void AddNamedParameter(string name, object value)
        {
            _command.AddNamedParameter(name, value);
        }

        public void UseParameter(NpgsqlParameter parameter)
        {
            var sql = _sql.ToString();
            _sql.Clear();
            _sql.Append(sql.UseParameter(parameter));
        }

        public NpgsqlParameter[] AppendWithParameters(string text)
        {
            var split = text.Split('?');
            var parameters = new NpgsqlParameter[split.Length - 1];

            _sql.Append(split[0]);
            for (var i = 0; i < parameters.Length; i++)
            {
                var parameter = _command.AddParameter(DBNull.Value);
                parameters[i] = parameter;
                _sql.Append(':');
                _sql.Append(parameter.ParameterName);
                _sql.Append(split[i + 1]);
            }

            return parameters;
        }

        public Task<NpgsqlDataReader> ExecuteReaderAsync(NpgsqlConnection conn, CancellationToken cancellation = default, NpgsqlTransaction tx = null)
        {
            var cmd = Compile();
            cmd.Connection = conn;
            cmd.Transaction = tx;

            return cmd.ExecuteReaderAsync(cancellation);
        }
    }
}
