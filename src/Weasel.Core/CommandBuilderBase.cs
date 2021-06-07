using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Weasel.Core
{
    public class CommandBuilderBase<TCommand, TParameter, TConnection, TTransaction, TParameterType, TDataReader>
        where TCommand : DbCommand, new()
        where TParameter : DbParameter
        where TConnection : DbConnection
        where TTransaction : DbTransaction
        where TDataReader : DbDataReader
        where TParameterType : struct
    {
        protected readonly IDatabaseProvider<TCommand, TParameter, TConnection, TTransaction, TParameterType, TDataReader> _mappings;
        protected readonly char _parameterPrefix;
        protected readonly TCommand _command;
        
        // TEMP -- will shift this to being pooled later
        protected readonly StringBuilder _sql = new();


        protected CommandBuilderBase(IDatabaseProvider<TCommand, TParameter, TConnection, TTransaction, TParameterType, TDataReader> mappings, char parameterPrefix, TCommand command)
        {
            _mappings = mappings;
            _parameterPrefix = parameterPrefix;
            _command = command;
        }
        
        public void Append(string text)
        {
            _sql.Append(text);
        }

        public void Append(char character)
        {
            _sql.Append(character);
        }
        
        public TCommand Compile()
        {
            _command.CommandText = _sql.ToString();
            return _command;
        }
        
        public Task<int> ExecuteNonQueryAsync(TConnection conn, CancellationToken cancellation = default,
            TTransaction? tx = null)
        {
            var cmd = Compile();
            cmd.Connection = conn;
            cmd.Transaction = tx;

            return cmd.ExecuteNonQueryAsync(cancellation);
        }
        
        public async Task<TDataReader> ExecuteReaderAsync(TConnection conn,
            CancellationToken cancellation = default, TTransaction? tx = null)
        {
            var cmd = Compile();
            cmd.Connection = conn;
            cmd.Transaction = tx;

            return (TDataReader) await cmd.ExecuteReaderAsync(cancellation);
        }

        public async Task<IReadOnlyList<T>> FetchList<T>(TConnection conn, Func<DbDataReader, Task<T>> transform,
            CancellationToken cancellation = default, TTransaction? tx = null)
        {
            var cmd = Compile();
            cmd.Connection = conn;
            cmd.Transaction = tx;
            
            var list = new List<T>();

            using var reader = await cmd.ExecuteReaderAsync(cancellation);
            while (await reader.ReadAsync(cancellation))
            {
                list.Add(await transform(reader));
            }

            return list;
        }
    }
}