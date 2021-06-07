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
        
        public static T Sql<T>(this T cmd, string sql) where T : DbCommand
        {
            cmd.CommandText = sql;
            return cmd;
        }
        
        
        public static T CallsSproc<T>(this T cmd, string functionName) where T : DbCommand
        {
            cmd.CommandText = functionName;
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

    }
}