using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using Weasel.Postgresql.SqlGeneration;

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

        public void Append(string text)
        {
            _sql.Append(text);
        }
        
        /// <summary>
        /// Append a parameter with the supplied value to the underlying command
        /// </summary>
        /// <param name="value"></param>
        /// <param name="dbType"></param>
        public void AppendParameter(object value)
        {
            var dbType = TypeMappings.ToDbType(value.GetType());
            AppendParameter(value, dbType);
        }
        
        /// <summary>
        /// Append a parameter with the supplied value to the underlying command
        /// </summary>
        /// <param name="value"></param>
        /// <param name="dbType"></param>
        public void AppendParameter(int value)
        {
            AppendParameter(value, NpgsqlDbType.Integer);
        }
        
        /// <summary>
        /// Append a parameter with the supplied value to the underlying command
        /// </summary>
        /// <param name="value"></param>
        /// <param name="dbType"></param>
        public void AppendParameter(Guid value)
        {
            AppendParameter(value, NpgsqlDbType.Uuid);
        }

        /// <summary>
        /// Append a parameter with the supplied value to the underlying command
        /// </summary>
        /// <param name="value"></param>
        /// <param name="dbType"></param>
        public void AppendParameter(object value, NpgsqlDbType dbType)
        {
            var parameter = AddParameter(value, dbType);
            Append(":");
            Append(parameter.ParameterName);
        }
        
        /// <summary>
        /// Append a parameter with the supplied value to the underlying command parameter
        /// collection and adds the parameter usage to the SQL
        /// </summary>
        /// <param name="value"></param>
        /// <param name="dbType"></param>
        public void AppendParameter(string value)
        {
            AppendParameter(value, NpgsqlDbType.Varchar);
        }
        
        /// <summary>
        /// Append a parameter with the supplied value to the underlying command parameter
        /// collection and adds the parameter usage to the SQL
        /// </summary>
        /// <param name="value"></param>
        /// <param name="dbType"></param>
        public void AppendParameter(string[] values)
        {
            AppendParameter(values, NpgsqlDbType.Varchar | NpgsqlDbType.Array);
        }

        public void Append(object o)
        {
            _sql.Append(o);
        }

        public override string ToString()
        {
            return _sql.ToString();
        }

        public void AddParameters(object parameters)
        {
            _command.AddParameters(parameters);
        }

        /// <summary>
        /// Adds a parameter to the underlying command, but does NOT add the
        /// parameter usage to the command text
        /// </summary>
        /// <param name="value"></param>
        /// <param name="dbType"></param>
        /// <returns></returns>
        public NpgsqlParameter AddParameter(object value, NpgsqlDbType? dbType = null)
        {
            return _command.AddParameter(value, dbType);
        }

        public NpgsqlParameter AddJsonParameter(string json)
        {
            return _command.AddParameter(json, NpgsqlDbType.Jsonb);
        }

        public NpgsqlParameter AddNamedParameter(string name, object value)
        {
            return _command.AddNamedParameter(name, value);
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

        public Task<IReadOnlyList<T>> FetchList<T>(NpgsqlConnection conn, Func<DbDataReader, Task<T>> transform, CancellationToken cancellation = default, NpgsqlTransaction tx = null)
        {
            var cmd = Compile();
            cmd.Connection = conn;
            cmd.Transaction = tx;

            return cmd.FetchList(transform, cancellation: cancellation);
        }

        public Task<int> ExecuteNonQueryAsync(NpgsqlConnection conn, CancellationToken cancellation = default, NpgsqlTransaction tx = null)
        {
            var cmd = Compile();
            cmd.Connection = conn;
            cmd.Transaction = tx;

            return cmd.ExecuteNonQueryAsync(cancellation);
        }

        public void AppendJsonBParameter(string json)
        {
            AppendParameter(json, NpgsqlDbType.Jsonb);
        }
    }
}
