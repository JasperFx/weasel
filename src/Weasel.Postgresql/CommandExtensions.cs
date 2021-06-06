using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Baseline;
using Npgsql;
using NpgsqlTypes;

namespace Weasel.Postgresql
{
    public static class CommandExtensions
    {
        public static Task<int> RunSql(this NpgsqlConnection conn, params string[] sqls)
        {
            var sql = sqls.Join(";");
            return conn.CreateCommand(sql).ExecuteNonQueryAsync();
        }

        public static async Task<IReadOnlyList<T>> FetchList<T>(this NpgsqlCommand cmd, Func<DbDataReader, Task<T>> transform, CancellationToken cancellation = default)
        {
            var list = new List<T>();

            await using var reader = await cmd.ExecuteReaderAsync(cancellation);
            while (await reader.ReadAsync(cancellation))
            {
                list.Add(await transform(reader));
            }

            return list;
        }
        
        public static async Task<T> FetchOne<T>(this NpgsqlCommand cmd, CancellationToken cancellation = default)
        {
            using var reader = await cmd.ExecuteReaderAsync(cancellation);
            if (await reader.ReadAsync(cancellation))
            {
                if (await reader.IsDBNullAsync(0, cancellation))
                {
                    return default;
                }

                var result = await reader.GetFieldValueAsync<T>(0, cancellation);
                return result;
            }

            return default;
        }
        
        public static Task<IReadOnlyList<T>> FetchList<T>(this NpgsqlCommand cmd, CancellationToken cancellation = default)
        {
            return cmd.FetchList(async reader =>
            {
                if (await reader.IsDBNullAsync(0, cancellation))
                {
                    return default;
                }

                return await reader.GetFieldValueAsync<T>(0, cancellation);
            }, cancellation);
        }

        public static string UseParameter(this string text, NpgsqlParameter parameter)
        {
            return text.ReplaceFirst("?", ":" + parameter.ParameterName);
        }


        public static NpgsqlParameter AddParameter(this NpgsqlCommand command, object value, NpgsqlDbType? dbType = null)
        {
            var name = "p" + command.Parameters.Count;

            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;

            if (dbType.HasValue)
            {
                parameter.NpgsqlDbType = dbType.Value;
            }

            command.Parameters.Add(parameter);

            return parameter;
        }


        /// <summary>
        /// Finds or adds a new parameter with the specified name and returns the parameter
        /// </summary>
        /// <param name="command"></param>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <param name="dbType"></param>
        /// <returns></returns>
        public static NpgsqlParameter AddNamedParameter(this NpgsqlCommand command, string name, object value, NpgsqlDbType? dbType = null)
        {
            var existing = command.Parameters.FirstOrDefault(x => x.ParameterName == name);
            if (existing != null) return existing;

            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;


            if (dbType.HasValue)
            {
                parameter.NpgsqlDbType = dbType.Value;
            }
            else if (value != null)
            {
                dbType = TypeMappings.TryGetDbType(value.GetType());
                parameter.NpgsqlDbType = dbType.Value;
            }
            
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);

            return parameter;
        }
        


        public static NpgsqlCommand With(this NpgsqlCommand command, string name, Guid value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.NpgsqlDbType = NpgsqlDbType.Uuid;
            parameter.Value = value;

            command.Parameters.Add(parameter);

            return command;
        }

        public static NpgsqlCommand With(this NpgsqlCommand command, string name, string value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.NpgsqlDbType = NpgsqlDbType.Varchar;

            if (value == null)
            {
                parameter.Value = DBNull.Value;
            }
            else
            {
                parameter.Value = value;
            }

            command.Parameters.Add(parameter);

            return command;
        }

        public static NpgsqlCommand With(this NpgsqlCommand command, string name, object value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);

            return command;
        }
        
        public static NpgsqlCommand With(this NpgsqlCommand command, string name, string[] value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;

            if (value == null)
            {
                parameter.Value = DBNull.Value;
            }
            else
            {
                parameter.Value = value;
            }
            
            parameter.NpgsqlDbType = NpgsqlDbType.Varchar | NpgsqlDbType.Array;
            command.Parameters.Add(parameter);

            return command;
        }

        public static NpgsqlCommand With(this NpgsqlCommand command, string name, object value, NpgsqlDbType dbType)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;
            parameter.NpgsqlDbType = dbType;
            command.Parameters.Add(parameter);

            return command;
        }

        public static NpgsqlCommand WithJsonParameter(this NpgsqlCommand command, string name, string json)
        {
            command.Parameters.Add(name, NpgsqlDbType.Jsonb).Value = json;

            return command;
        }

        public static NpgsqlCommand Sql(this NpgsqlCommand cmd, string sql)
        {
            cmd.CommandText = sql;
            return cmd;
        }


        public static NpgsqlCommand Returns(this NpgsqlCommand command, string name, NpgsqlDbType type)
        {
            var parameter = command.AddParameter(name);
            parameter.NpgsqlDbType = type;
            parameter.Direction = ParameterDirection.ReturnValue;
            return command;
        }

        public static NpgsqlCommand WithText(this NpgsqlCommand command, string sql)
        {
            command.CommandText = sql;
            return command;
        }

        public static NpgsqlCommand CreateCommand(this NpgsqlConnection conn, string command, NpgsqlTransaction tx = null)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = command;
            cmd.Transaction = tx;

            return cmd;
        }
        
        
        public static NpgsqlCommand CallsSproc(this NpgsqlCommand cmd, DbObjectName function)
        {
            if (cmd == null)
                throw new ArgumentNullException(nameof(cmd));
            if (function == null)
                throw new ArgumentNullException(nameof(function));

            cmd.CommandText = function.QualifiedName;
            cmd.CommandType = CommandType.StoredProcedure;

            return cmd;
        }


    }
}
