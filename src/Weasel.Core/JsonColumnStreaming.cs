using System.Collections.Concurrent;
using System.Data.Common;

namespace Weasel.Core;

/// <summary>
///     Decides whether a given <see cref="DbDataReader" /> implementation can hand back a JSON
///     column as a <see cref="Stream" />, so the read path can skip materializing a full-size
///     UTF-16 string that System.Text.Json would only transcode back to UTF-8 to parse.
/// </summary>
/// <remarks>
///     <para>
///         The answer is per provider and cannot be read off the API surface: every
///         <see cref="DbDataReader" /> has <c>GetStream</c>, but only some will use it for a text
///         column. Measured against live databases (weasel#565):
///     </para>
///     <list type="table">
///         <item>
///             <term>Microsoft.Data.Sqlite</term>
///             <description>
///                 Never asked (weasel#863). <c>GetStream</c> on a column with no origin table (a
///                 literal, a <c>json_object(...)</c>, any expression) beside another computed column
///                 calls <c>sqlite3_table_column_metadata</c> with a null table name and takes the
///                 process down with a SIGSEGV, which no <c>catch</c> can turn into a fallback. The
///                 provider serves blob I/O per call anyway, so the string path costs nothing
///                 measurable there (fisher#546, fisher#555).
///             </description>
///         </item>
///         <item><term>Npgsql</term><description><c>jsonb</c>, <c>json</c>, <c>text</c> — all stream.</description></item>
///         <item>
///             <term>Microsoft.Data.SqlClient</term>
///             <description>
///                 <c>nvarchar(max)</c> and SQL Server 2025's native <c>json</c> both throw
///                 <c>InvalidCastException</c>: "can only be used on columns of type Binary, Image,
///                 Udt or VarBinary". SQL Server also sends both as UTF-16 on the wire, so there is
///                 no UTF-8 to reach for — the string path is already the right one there.
///             </description>
///         </item>
///     </list>
///     <para>
///         Rather than enumerate providers — which would be wrong for the next one — the capability
///         is learned once per reader type by attempting the call and remembering the refusal. That
///         costs one exception per reader type per process, and is safe because a failed
///         <c>GetStream</c> does not consume the column: <c>GetString</c> on the same column
///         afterwards still returns the value, under both <c>CommandBehavior.Default</c> and
///         <c>SequentialAccess</c> (also measured in weasel#565). Microsoft.Data.Sqlite is the one
///         provider named up front, because its failure is a crash rather than a refusal.
///     </para>
///     <para>
///         Only the accessor call itself is guarded. An <see cref="InvalidCastException" /> thrown
///         from inside deserialization — by a custom converter, say — must not be mistaken for a
///         provider that cannot stream, so deserialization happens outside the guard.
///     </para>
/// </remarks>
internal static class JsonColumnStreaming
{
    private static readonly ConcurrentDictionary<Type, bool> _supported = new();

    // Weasel.Core does not reference Microsoft.Data.Sqlite, so the reader is recognised by name.
    private const string SqliteDataReaderTypeName = "Microsoft.Data.Sqlite.SqliteDataReader";

    /// <summary>
    ///     The column's value as a stream, or null when this provider will not stream it — in which
    ///     case the caller should fall back to <see cref="DbDataReader.GetString" />.
    /// </summary>
    public static Stream? TryGetStream(DbDataReader reader, int index)
    {
        var readerType = reader.GetType();
        if (isKnownNotToStream(readerType))
        {
            return null;
        }

        try
        {
            var stream = reader.GetStream(index);
            _supported[readerType] = true;
            return stream;
        }
        catch (Exception e) when (e is InvalidCastException or NotSupportedException)
        {
            _supported[readerType] = false;
            return null;
        }
    }

    /// <summary>
    ///     The async counterpart of <see cref="TryGetStream" />, going through
    ///     <c>GetFieldValueAsync&lt;Stream&gt;</c> so a provider that streams asynchronously can.
    /// </summary>
    public static async ValueTask<Stream?> TryGetStreamAsync(DbDataReader reader, int index,
        CancellationToken cancellationToken)
    {
        var readerType = reader.GetType();
        if (isKnownNotToStream(readerType))
        {
            return null;
        }

        try
        {
            var stream = await reader.GetFieldValueAsync<Stream>(index, cancellationToken).ConfigureAwait(false);
            _supported[readerType] = true;
            return stream;
        }
        catch (Exception e) when (e is InvalidCastException or NotSupportedException)
        {
            _supported[readerType] = false;
            return null;
        }
    }

    /// <summary>
    ///     True when this reader type has already refused, or must never be asked because asking
    ///     can crash the process rather than throw (Microsoft.Data.Sqlite, weasel#863). The SQLite
    ///     answer is decided once per reader type and remembered in the same cache as a refusal.
    /// </summary>
    private static bool isKnownNotToStream(Type readerType)
    {
        if (_supported.TryGetValue(readerType, out var supported))
        {
            return !supported;
        }

        for (var type = readerType; type != null; type = type.BaseType)
        {
            if (type.FullName == SqliteDataReaderTypeName)
            {
                _supported[readerType] = false;
                return true;
            }
        }

        return false;
    }
}
