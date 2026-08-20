using System.Collections;
using System.Data.Common;

namespace Weasel.Core;

/// <summary>
///     Presents an ordered list of commands as one continuous sequence of result sets:
///     <see cref="NextResult" /> walks the current command's result sets and then rolls over into
///     the next command's.
/// </summary>
/// <remarks>
///     <para>
///         Every Weasel provider except Oracle can execute a whole batch from a single
///         <see cref="DbCommand" /> and hand back one result set per statement, which is what
///         <c>SchemaMigration.DetermineAsync</c> assumes. ODP.NET cannot: it does not implement the
///         ADO.NET batching API and Oracle will not execute several semicolon-separated statements
///         from one command. <see cref="CommandBuilderBase.CompileCommands" /> already splits an
///         Oracle batch into one command per statement; this is what makes the split invisible to
///         the code reading the results (weasel#474).
///     </para>
///     <para>
///         Only used when there is more than one command. A single-command batch executes exactly
///         as it always did, so the providers that never split pay nothing for this.
///     </para>
/// </remarks>
internal sealed class MultiCommandDataReader: DbDataReader
{
    private readonly IReadOnlyList<DbCommand> _commands;
    private DbDataReader _current;
    private int _index;

    private MultiCommandDataReader(IReadOnlyList<DbCommand> commands, DbDataReader first)
    {
        _commands = commands;
        _current = first;
        _index = 0;
    }

    public static async Task<DbDataReader> OpenAsync(
        IReadOnlyList<DbCommand> commands,
        CancellationToken ct = default)
    {
        var first = await commands[0].ExecuteReaderAsync(ct).ConfigureAwait(false);
        return new MultiCommandDataReader(commands, first);
    }

    /// <summary>
    ///     Advance to the next result set, crossing into the next command when the current one is
    ///     exhausted. A freshly executed reader is positioned before its first row, which is exactly
    ///     where <see cref="DbDataReader.NextResult" /> leaves a caller, so the rollover is
    ///     indistinguishable from an ordinary result set boundary.
    /// </summary>
    public override async Task<bool> NextResultAsync(CancellationToken ct)
    {
        if (await _current.NextResultAsync(ct).ConfigureAwait(false))
        {
            return true;
        }

        if (_index + 1 >= _commands.Count)
        {
            return false;
        }

        await _current.DisposeAsync().ConfigureAwait(false);
        _index++;
        _current = await _commands[_index].ExecuteReaderAsync(ct).ConfigureAwait(false);

        return true;
    }

    public override bool NextResult()
    {
        if (_current.NextResult())
        {
            return true;
        }

        if (_index + 1 >= _commands.Count)
        {
            return false;
        }

        _current.Dispose();
        _index++;
        _current = _commands[_index].ExecuteReader();

        return true;
    }

    public override async Task<bool> ReadAsync(CancellationToken ct) =>
        await _current.ReadAsync(ct).ConfigureAwait(false);

    public override bool Read() => _current.Read();

    public override async Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken ct) =>
        await _current.GetFieldValueAsync<T>(ordinal, ct).ConfigureAwait(false);

    public override async Task<bool> IsDBNullAsync(int ordinal, CancellationToken ct) =>
        await _current.IsDBNullAsync(ordinal, ct).ConfigureAwait(false);

    public override async Task CloseAsync()
    {
        await _current.CloseAsync().ConfigureAwait(false);
    }

    public override void Close() => _current.Close();

    public override async ValueTask DisposeAsync()
    {
        await _current.DisposeAsync().ConfigureAwait(false);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _current.Dispose();
        }
    }

    // Everything below is plain delegation to whichever reader is current.

    public override int Depth => _current.Depth;
    public override int FieldCount => _current.FieldCount;
    public override bool HasRows => _current.HasRows;
    public override bool IsClosed => _current.IsClosed;
    public override int RecordsAffected => _current.RecordsAffected;
    public override object this[int ordinal] => _current[ordinal];
    public override object this[string name] => _current[name];

    public override bool GetBoolean(int ordinal) => _current.GetBoolean(ordinal);
    public override byte GetByte(int ordinal) => _current.GetByte(ordinal);

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) =>
        _current.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);

    public override char GetChar(int ordinal) => _current.GetChar(ordinal);

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) =>
        _current.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);

    public override string GetDataTypeName(int ordinal) => _current.GetDataTypeName(ordinal);
    public override DateTime GetDateTime(int ordinal) => _current.GetDateTime(ordinal);
    public override decimal GetDecimal(int ordinal) => _current.GetDecimal(ordinal);
    public override double GetDouble(int ordinal) => _current.GetDouble(ordinal);
    public override Type GetFieldType(int ordinal) => _current.GetFieldType(ordinal);
    public override float GetFloat(int ordinal) => _current.GetFloat(ordinal);
    public override Guid GetGuid(int ordinal) => _current.GetGuid(ordinal);
    public override short GetInt16(int ordinal) => _current.GetInt16(ordinal);
    public override int GetInt32(int ordinal) => _current.GetInt32(ordinal);
    public override long GetInt64(int ordinal) => _current.GetInt64(ordinal);
    public override string GetName(int ordinal) => _current.GetName(ordinal);
    public override int GetOrdinal(string name) => _current.GetOrdinal(name);
    public override string GetString(int ordinal) => _current.GetString(ordinal);
    public override object GetValue(int ordinal) => _current.GetValue(ordinal);
    public override int GetValues(object[] values) => _current.GetValues(values);
    public override bool IsDBNull(int ordinal) => _current.IsDBNull(ordinal);
    public override IEnumerator GetEnumerator() => _current.GetEnumerator();
}
