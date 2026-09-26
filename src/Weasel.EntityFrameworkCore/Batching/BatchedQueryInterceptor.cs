using System.Collections;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     Lets <see cref="BatchedQuery" /> send its queries in one round trip while EF Core still
///     materializes every result. When EF Core runs a batched query, the interceptor supplies that
///     query's result set from the batch instead of executing the command.
///     Register it with <see cref="BatchQueryExtensions.UseWeaselBatchedQueries(DbContextOptionsBuilder)" />.
/// </summary>
public sealed class BatchedQueryInterceptor : DbCommandInterceptor
{
    public static BatchedQueryInterceptor Instance { get; } = new();

    private static readonly ConditionalWeakTable<DbContext, PendingResult> Pending = new();

    private sealed record PendingResult(DbDataReader Reader, string CommandText);

    internal static bool IsRegistered(DbContext context) =>
        context.GetService<IDbContextOptions>().FindExtension<CoreOptionsExtension>()?.Interceptors?
            .OfType<BatchedQueryInterceptor>().Any() == true;

    internal static void Supply(DbContext context, DbDataReader reader, string commandText) =>
        Pending.AddOrUpdate(context, new PendingResult(new ResultSetReader(reader), commandText));

    internal static void Clear(DbContext context) => Pending.Remove(context);

    public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData,
        InterceptionResult<DbDataReader> result) => Take(command, eventData, result);

    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command,
        CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
        => new(Take(command, eventData, result));

    private static InterceptionResult<DbDataReader> Take(DbCommand command, CommandEventData eventData,
        InterceptionResult<DbDataReader> result)
    {
        // Only the command the batch ran may read its result set. Any other command EF Core issues
        // while materializing executes normally, and fails loudly on the busy connection.
        if (eventData.Context == null || !Pending.TryGetValue(eventData.Context, out var pending) ||
            pending.CommandText != command.CommandText)
        {
            return result;
        }

        Pending.Remove(eventData.Context);
        return InterceptionResult<DbDataReader>.SuppressWithResult(pending.Reader);
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
        [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
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
        [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
        public override Type GetProviderSpecificFieldType(int ordinal) => inner.GetProviderSpecificFieldType(ordinal);
        public override object GetProviderSpecificValue(int ordinal) => inner.GetProviderSpecificValue(ordinal);
        public override int GetProviderSpecificValues(object[] values) => inner.GetProviderSpecificValues(values);
        public override System.Data.DataTable? GetSchemaTable() => inner.GetSchemaTable();
        public override IEnumerator GetEnumerator() => ((IEnumerable)inner).GetEnumerator();
    }
}
