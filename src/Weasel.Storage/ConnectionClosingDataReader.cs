#nullable enable
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Weasel.Storage;

/// <summary>
///     A <see cref="DbDataReader" /> that owns its connection and closes it when the reader is closed or
///     disposed — what <see cref="CommandBehavior.CloseConnection" /> promises, for the places a driver
///     does not keep that promise.
/// </summary>
/// <remarks>
///     <para>
///         Microsoft.Data.SqlClient honours <c>CloseConnection</c> for <c>SqlCommand</c> readers and
///         silently ignores it for <c>SqlBatch</c> ones, so a session that opened a connection for one
///         batched read and relied on the flag leaked the pooled connection on every read. The symptom is
///         the pool's ceiling, far from the cause: a timeout obtaining a connection on whichever query is
///         the one past the limit.
///     </para>
///     <para>
///         The connection is disposed even when disposing the inner reader throws; leaking it is the
///         failure this type exists to prevent.
///     </para>
///     <para>
///         Code that needs the driver's concrete reader must unwrap it through <see cref="Unwrap" /> rather
///         than cast: the parameter is typed as <see cref="DbDataReader" />, so a cast breaks at run time,
///         not at compile time.
///     </para>
/// </remarks>
public sealed class ConnectionClosingDataReader: DbDataReader
{
    private readonly DbDataReader _inner;
    private readonly DbConnection _connection;
    private bool _disposed;

    /// <param name="inner">The reader to wrap.</param>
    /// <param name="connection">The connection the reader was opened on, closed with it.</param>
    public ConnectionClosingDataReader(DbDataReader inner, DbConnection connection)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    /// <summary>The wrapped reader.</summary>
    public DbDataReader Inner => _inner;

    /// <summary>
    ///     The innermost reader behind any number of <see cref="ConnectionClosingDataReader" /> wrappers.
    /// </summary>
    public static DbDataReader Unwrap(DbDataReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);

        while (reader is ConnectionClosingDataReader wrapper)
        {
            reader = wrapper._inner;
        }

        return reader;
    }

    /// <inheritdoc />
    public override int Depth => _inner.Depth;
    /// <inheritdoc />
    public override int FieldCount => _inner.FieldCount;
    /// <inheritdoc />
    public override int VisibleFieldCount => _inner.VisibleFieldCount;
    /// <inheritdoc />
    public override bool HasRows => _inner.HasRows;
    /// <inheritdoc />
    public override bool IsClosed => _inner.IsClosed;
    /// <inheritdoc />
    public override int RecordsAffected => _inner.RecordsAffected;
    /// <inheritdoc />
    public override object this[int ordinal] => _inner[ordinal];
    /// <inheritdoc />
    public override object this[string name] => _inner[name];

    /// <inheritdoc />
    public override bool GetBoolean(int ordinal) => _inner.GetBoolean(ordinal);
    /// <inheritdoc />
    public override byte GetByte(int ordinal) => _inner.GetByte(ordinal);

    /// <inheritdoc />
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        => _inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);

    /// <inheritdoc />
    public override char GetChar(int ordinal) => _inner.GetChar(ordinal);

    /// <inheritdoc />
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        => _inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);

    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal) => _inner.GetDataTypeName(ordinal);
    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal) => _inner.GetDateTime(ordinal);
    /// <inheritdoc />
    public override decimal GetDecimal(int ordinal) => _inner.GetDecimal(ordinal);
    /// <inheritdoc />
    public override double GetDouble(int ordinal) => _inner.GetDouble(ordinal);
    /// <inheritdoc />
    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type GetFieldType(int ordinal) => _inner.GetFieldType(ordinal);
    /// <inheritdoc />
    public override float GetFloat(int ordinal) => _inner.GetFloat(ordinal);
    /// <inheritdoc />
    public override Guid GetGuid(int ordinal) => _inner.GetGuid(ordinal);
    /// <inheritdoc />
    public override short GetInt16(int ordinal) => _inner.GetInt16(ordinal);
    /// <inheritdoc />
    public override int GetInt32(int ordinal) => _inner.GetInt32(ordinal);
    /// <inheritdoc />
    public override long GetInt64(int ordinal) => _inner.GetInt64(ordinal);
    /// <inheritdoc />
    public override string GetName(int ordinal) => _inner.GetName(ordinal);
    /// <inheritdoc />
    public override int GetOrdinal(string name) => _inner.GetOrdinal(name);
    /// <inheritdoc />
    public override string GetString(int ordinal) => _inner.GetString(ordinal);
    /// <inheritdoc />
    public override object GetValue(int ordinal) => _inner.GetValue(ordinal);
    /// <inheritdoc />
    public override int GetValues(object[] values) => _inner.GetValues(values);
    /// <inheritdoc />
    public override bool IsDBNull(int ordinal) => _inner.IsDBNull(ordinal);
    /// <inheritdoc />
    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type GetProviderSpecificFieldType(int ordinal) => _inner.GetProviderSpecificFieldType(ordinal);
    /// <inheritdoc />
    public override object GetProviderSpecificValue(int ordinal) => _inner.GetProviderSpecificValue(ordinal);
    /// <inheritdoc />
    public override int GetProviderSpecificValues(object[] values) => _inner.GetProviderSpecificValues(values);

    /// <inheritdoc />
    public override T GetFieldValue<T>(int ordinal) => _inner.GetFieldValue<T>(ordinal);

    /// <inheritdoc />
    public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
        => _inner.GetFieldValueAsync<T>(ordinal, cancellationToken);

    /// <inheritdoc />
    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
        => _inner.IsDBNullAsync(ordinal, cancellationToken);

    /// <inheritdoc />
    public override Stream GetStream(int ordinal) => _inner.GetStream(ordinal);
    /// <inheritdoc />
    public override TextReader GetTextReader(int ordinal) => _inner.GetTextReader(ordinal);
    /// <inheritdoc />
    public override DataTable? GetSchemaTable() => _inner.GetSchemaTable();

    /// <inheritdoc />
    public override Task<DataTable?> GetSchemaTableAsync(CancellationToken cancellationToken = default)
        => _inner.GetSchemaTableAsync(cancellationToken);

    /// <inheritdoc />
    public override bool Read() => _inner.Read();

    /// <inheritdoc />
    public override Task<bool> ReadAsync(CancellationToken cancellationToken) => _inner.ReadAsync(cancellationToken);

    /// <inheritdoc />
    public override bool NextResult() => _inner.NextResult();

    /// <inheritdoc />
    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        => _inner.NextResultAsync(cancellationToken);

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => _inner.GetEnumerator();

    /// <inheritdoc />
    public override void Close()
    {
        try
        {
            _inner.Close();
        }
        finally
        {
            _connection.Close();
        }
    }

    /// <inheritdoc />
    public override async Task CloseAsync()
    {
        try
        {
            await _inner.CloseAsync().ConfigureAwait(false);
        }
        finally
        {
            await _connection.CloseAsync().ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (!disposing)
        {
            return;
        }

        try
        {
            _inner.Dispose();
        }
        finally
        {
            _connection.Dispose();
        }
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        try
        {
            await _inner.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }

        GC.SuppressFinalize(this);
    }
}
