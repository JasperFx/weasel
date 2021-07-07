using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Baseline;

namespace Weasel.Core
{



    public static class CommandExtensions
    {
        
        public static DbCommandBuilder ToCommandBuilder(this DbConnection connection)
        {
            return new DbCommandBuilder(connection);
        }


        public static T Sql<T>(this T cmd, string sql) where T : DbCommand
        {
            cmd.CommandText = sql;
            return cmd;
        }
        
        
        public static T CallsSproc<T>(this T cmd, string storedProcedureName) where T : DbCommand
        {
            cmd.CommandText = storedProcedureName;
            cmd.CommandType = CommandType.StoredProcedure;

            return cmd;
        }

        
        public static Task<int> RunSql(this DbConnection conn, params string[] sqls)
        {
            var sql = sqls.Join(";");
            return conn.CreateCommand(sql).ExecuteNonQueryAsync();
        }

        public static DbCommand CreateCommand(this DbConnection conn, string sql)
        {
            var command = conn.CreateCommand();
            command.CommandText = sql;
            return command;
        }
        
        public static async Task<IReadOnlyList<T>> FetchList<T>(this DbCommand cmd, Func<DbDataReader, Task<T>> transform, CancellationToken cancellation = default)
        {
            var list = new List<T>();

            using var reader = await cmd.ExecuteReaderAsync(cancellation);
            while (await reader.ReadAsync(cancellation))
            {
                list.Add(await transform(reader));
            }

            return list;
        }
        
        public static Task<IReadOnlyList<T>> FetchList<T>(this DbCommand cmd, CancellationToken cancellation = default)
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
        
        
        
        public static async Task<T> FetchOne<T>(this DbCommand cmd, CancellationToken cancellation = default)
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
        
        /// <summary>
        /// Open the attached connection, execute the command, and close the connection
        /// in one call. This assumes that the connection is not already open
        /// </summary>
        /// <param name="command"></param>
        /// <param name="cancellation"></param>
        public static async Task ExecuteOnce(this DbCommand command, CancellationToken cancellation = default)
        {
            if (cancellation.IsCancellationRequested) return;

            if (command.Connection is not {State: ConnectionState.Closed})
                throw new InvalidOperationException(
                    "The command must have an attached, but not yet open connection to use this extension method");

            var conn = command.Connection;
            try
            {
                await conn.OpenAsync(cancellation);

                await command.ExecuteNonQueryAsync(cancellation);
            }
            finally
            {
                conn.Close();
            }
        }
        
        
        public static DbParameter AddParameter(this DbCommand command, object value, DbType? dbType = null)
        {
            var name = "arg" + command.Parameters.Count;

            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;

            if (dbType.HasValue) parameter.DbType = dbType.Value;

            command.Parameters.Add(parameter);

            return parameter;
        }

        public static DbParameter AddNamedParameter(this DbCommand command, string name, object value,
            DbType? type = null)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;

            if (type.HasValue) parameter.DbType = type.Value;

            command.Parameters.Add(parameter);

            return parameter;
        }

        public static DbCommand With(this DbCommand command, string name, object value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);

            return command;
        }

        public static DbCommand With(this DbCommand command, string name, object value, DbType dbType)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;
            parameter.DbType = dbType;
            command.Parameters.Add(parameter);

            return command;
        }

        public static DbCommand With(this DbCommand command, string name, string value)
        {
            return command.With(name, value, DbType.String);
        }

        public static DbCommand With(this DbCommand command, string name, int value)
        {
            return command.With(name, value, DbType.Int32);
        }

        public static DbCommand With(this DbCommand command, string name, Guid value)
        {
            return command.With(name, value, DbType.Guid);
        }

        public static DbCommand With(this DbCommand command, string name, byte[] value)
        {
            return command.With(name, value, DbType.Binary);
        }

        public static DbCommand With(this DbCommand command, string name, DateTimeOffset? value)
        {
            return command.With(name, value, DbType.DateTimeOffset);
        }


    }
}