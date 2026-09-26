using System.Collections;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     Register with <c>optionsBuilder.AddInterceptors(new BatchedQueryInterceptor())</c> to let
///     <see cref="BatchedQuery" /> materialize results through EF Core's own query pipeline
///     (tracking, identity resolution, owned/complex/JSON members, includes, projections). The
///     interceptor hands the batch's result set to the query EF Core executes instead of running it.
/// </summary>
public sealed class BatchedQueryInterceptor : DbCommandInterceptor
{
    private static readonly ConditionalWeakTable<DbContext, DbDataReader> Pending = new();

    internal static bool IsRegistered(DbContext context) =>
        context.GetService<IDbContextOptions>().FindExtension<CoreOptionsExtension>()?.Interceptors?
            .OfType<BatchedQueryInterceptor>().Any() == true;

    internal static void Supply(DbContext context, DbDataReader reader) => Pending.AddOrUpdate(context, new ResultSetReader(reader));
    internal static void Clear(DbContext context) => Pending.Remove(context);

    public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData,
        InterceptionResult<DbDataReader> result) => Take(eventData, result);

    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command,
        CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
        => new(Take(eventData, result));

    private static InterceptionResult<DbDataReader> Take(CommandEventData eventData, InterceptionResult<DbDataReader> result)
    {
        if (eventData.Context == null || !Pending.TryGetValue(eventData.Context, out var reader)) return result;
        Pending.Remove(eventData.Context);
        return InterceptionResult<DbDataReader>.SuppressWithResult(reader);
    }

    /// <summary>The current result set of the batch reader; EF Core can read it but not advance or close it.</summary>
    private sealed class ResultSetReader(DbDataReader inner) : DbDataReader
    {
        public override bool NextResult() => false;
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);
        public override void Close() { }
        public override Task CloseAsync() => Task.CompletedTask;
        protected override void Dispose(bool disposing) { }
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public override bool Read() => inner.Read();
        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => inner.ReadAsync(cancellationToken);
        public override object this[int ordinal] => inner[ordinal];
        public override object this[string name] => inner[name];
        public override int Depth => inner.Depth;
        public override int FieldCount => inner.FieldCount;
        public override int VisibleFieldCount => inner.VisibleFieldCount;
        public override bool HasRows => inner.HasRows;
        public override bool IsClosed => inner.IsClosed;
        public override int RecordsAffected => inner.RecordsAffected;
        public override bool GetBoolean(int ordinal) => inner.GetBoolean(ordinal);
        public override byte GetByte(int ordinal) => inner.GetByte(ordinal);
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        public override char GetChar(int ordinal) => inner.GetChar(ordinal);
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        public override string GetDataTypeName(int ordinal) => inner.GetDataTypeName(ordinal);
        public override DateTime GetDateTime(int ordinal) => inner.GetDateTime(ordinal);
        public override decimal GetDecimal(int ordinal) => inner.GetDecimal(ordinal);
        public override double GetDouble(int ordinal) => inner.GetDouble(ordinal);
        public override Type GetFieldType(int ordinal) => inner.GetFieldType(ordinal);
        public override float GetFloat(int ordinal) => inner.GetFloat(ordinal);
        public override Guid GetGuid(int ordinal) => inner.GetGuid(ordinal);
        public override short GetInt16(int ordinal) => inner.GetInt16(ordinal);
        public override int GetInt32(int ordinal) => inner.GetInt32(ordinal);
        public override long GetInt64(int ordinal) => inner.GetInt64(ordinal);
        public override string GetName(int ordinal) => inner.GetName(ordinal);
        public override int GetOrdinal(string name) => inner.GetOrdinal(name);
        public override string GetString(int ordinal) => inner.GetString(ordinal);
        public override object GetValue(int ordinal) => inner.GetValue(ordinal);
        public override int GetValues(object[] values) => inner.GetValues(values);
        public override bool IsDBNull(int ordinal) => inner.IsDBNull(ordinal);
        public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) => inner.IsDBNullAsync(ordinal, cancellationToken);
        public override T GetFieldValue<T>(int ordinal) => inner.GetFieldValue<T>(ordinal);
        public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken) => inner.GetFieldValueAsync<T>(ordinal, cancellationToken);
        public override Stream GetStream(int ordinal) => inner.GetStream(ordinal);
        public override TextReader GetTextReader(int ordinal) => inner.GetTextReader(ordinal);
        public override Type GetProviderSpecificFieldType(int ordinal) => inner.GetProviderSpecificFieldType(ordinal);
        public override object GetProviderSpecificValue(int ordinal) => inner.GetProviderSpecificValue(ordinal);
        public override int GetProviderSpecificValues(object[] values) => inner.GetProviderSpecificValues(values);
        public override System.Data.DataTable? GetSchemaTable() => inner.GetSchemaTable();
        public override IEnumerator GetEnumerator() => ((IEnumerable)inner).GetEnumerator();
    }
}

/// <summary>A queued query that EF Core itself materializes from its result set in the batch.</summary>
internal sealed class EfPipelineBatchQueryItem<TResult>(DbContext context, DbCommand sourceCommand,
    Func<CancellationToken, Task<TResult>> execute) : IBatchQueryItem
{
    private readonly TaskCompletionSource<TResult> _completion = new();
    public Task<TResult> Result => _completion.Task;

    public void ConfigureCommand(DbBatchCommand command)
    {
        command.CommandText = sourceCommand.CommandText;
        foreach (DbParameter param in sourceCommand.Parameters)
        {
            var clone = command.CreateParameter();
            clone.ParameterName = param.ParameterName;
            clone.Value = param.Value;
            clone.DbType = param.DbType;
            clone.Direction = param.Direction;
            clone.Size = param.Size;
            command.Parameters.Add(clone);
        }
    }

    public async Task ReadAsync(DbDataReader reader, CancellationToken ct)
    {
        BatchedQueryInterceptor.Supply(context, reader);
        try
        {
            _completion.SetResult(await execute(ct).ConfigureAwait(false));
        }
        catch (Exception e)
        {
            _completion.SetException(e);
            throw;
        }
        finally
        {
            BatchedQueryInterceptor.Clear(context);
        }
    }
}
