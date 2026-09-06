using System.Buffers;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using Weasel.Benchmarks.Support;
using Weasel.Core;

namespace Weasel.Benchmarks;

/// <summary>
///     Reading one JSON document out of a <see cref="System.Data.Common.DbDataReader" /> column —
///     the whole path, query included.
/// </summary>
/// <remarks>
///     <para>
///         <b>What this is the baseline for.</b> <c>Weasel.Core.SystemTextJsonSerializer</c>'s
///         <c>FromJson&lt;T&gt;(DbDataReader, int)</c> calls <c>reader.GetString(index)</c> and hands
///         the result to STJ, which transcodes the fresh UTF-16 string straight back to UTF-8 to
///         parse it. Every document read and every event read therefore allocates a full-size string
///         that is discarded microseconds later. The <c>Current</c> row calls the real serializer, so
///         the same row can be re-run after the read path changes.
///     </para>
///     <para>
///         <b>Where the win actually is.</b> See <see cref="JsonDeserializeBenchmarks" />: STJ parses
///         UTF-8 substantially faster than UTF-16, so the prize is reaching the column's bytes
///         <i>without</i> building a string. <c>GetStream</c> does exactly that on providers that
///         store the column as UTF-8. Transcoding UTF-16 characters to UTF-8 by hand
///         (<c>PooledTextReader</c>) buys nothing — it just moves the transcode from inside STJ to
///         outside it, and pays for a buffer on the way. It is kept as a measured, rejected
///         candidate rather than deleted, because "why not just use GetTextReader everywhere" is the
///         obvious question and this row is the answer.
///     </para>
///     <para>
///         <b>Provider support for GetStream</b>, measured against live databases rather than assumed:
///         <list type="bullet">
///             <item>SQLite (Microsoft.Data.Sqlite), TEXT column — works.</item>
///             <item>PostgreSQL (Npgsql), <c>jsonb</c> / <c>json</c> / <c>text</c> — works.</item>
///             <item>
///                 SQL Server (Microsoft.Data.SqlClient), <c>nvarchar(max)</c> <b>and</b> SQL Server
///                 2025's native <c>json</c> — throws <c>InvalidCastException</c>: "can only be used
///                 on columns of type Binary, Image, Udt or VarBinary". SQL Server also sends these
///                 columns as UTF-16 on the wire, so there is no UTF-8 to reach for in the first
///                 place; the string path is already the right one there.
///             </item>
///         </list>
///     </para>
///     <para>
///         <b>Sizes.</b> 5 KB is the perf survey's reference document. 120 KB is there because the
///         gap between UTF-8 and UTF-16 parsing only opens up with size, and because 120 K characters
///         is 240 KB of UTF-16 — well past the 85 KB large-object threshold, so the string path
///         starts allocating on the LOH once per read.
///     </para>
/// </remarks>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class JsonReadBenchmarks
{
    private SystemTextJsonSerializer _serializer = null!;
    private string _path = null!;
    private SqliteConnection _connection = null!;
    private SqliteCommand _command = null!;

    /// <summary>Approximate JSON payload size in kilobytes.</summary>
    [Params(5, 120)]
    public int SizeKb { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _serializer = new SystemTextJsonSerializer();

        _path = Path.Combine(Path.GetTempPath(), "weasel-json-bench-" + Guid.NewGuid().ToString("N") + ".db");
        _connection = new SqliteConnection($"Data Source={_path};");
        await _connection.OpenAsync();

        var create = _connection.CreateCommand();
        create.CommandText = "create table docs (id integer primary key, doc text not null)";
        await create.ExecuteNonQueryAsync();

        var insert = _connection.CreateCommand();
        insert.CommandText = "insert into docs (id, doc) values (1, $d)";
        insert.Parameters.AddWithValue("$d", JsonPayload.Build(SizeKb));
        await insert.ExecuteNonQueryAsync();

        _command = _connection.CreateCommand();
        _command.CommandText = "select doc from docs where id = 1";
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        _command.Dispose();
        await _connection.DisposeAsync();
        SqliteConnection.ClearAllPools();
        try
        {
            File.Delete(_path);
        }
        catch (IOException)
        {
        }
    }

    [Benchmark(Baseline = true, Description = "Current: serializer.FromJson<T>(reader, index) [GetString + parse]")]
    public int Current()
    {
        using var reader = _command.ExecuteReader();
        reader.Read();
        return _serializer.FromJson<JsonPayload>(reader, 0).Items.Count;
    }

    [Benchmark(Description = "GetStream -> Deserialize(Stream) [SQLite + Npgsql only]")]
    public int Stream()
    {
        using var reader = _command.ExecuteReader();
        reader.Read();

        using var stream = reader.GetStream(0);
        return JsonSerializer.Deserialize<JsonPayload>(stream, _serializer.Options)!.Items.Count;
    }

    [Benchmark(Description = "Rejected: GetTextReader -> hand transcode -> Deserialize(span)")]
    public int PooledTextReader()
    {
        using var reader = _command.ExecuteReader();
        reader.Read();

        using var buffer = PooledUtf8.ReadFrom(reader, 0);
        return JsonSerializer.Deserialize<JsonPayload>(buffer.Span, _serializer.Options)!.Items.Count;
    }
}

/// <summary>
///     The deserialize step on its own, with the payload already in memory, so the provider and the
///     query are out of the picture. This is the measurement that says whether reaching the column's
///     UTF-8 bytes is worth any plumbing at all: if <c>ReadOnlySpan&lt;byte&gt;</c> and <c>string</c>
///     cost the same, no amount of stream routing in the serializer can help.
/// </summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class JsonDeserializeBenchmarks
{
    private string _json = null!;
    private byte[] _utf8 = null!;
    private JsonSerializerOptions _options = null!;

    /// <summary>Approximate JSON payload size in kilobytes.</summary>
    [Params(5, 120)]
    public int SizeKb { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _options = SystemTextJsonSerializer.DefaultOptions();
        _json = JsonPayload.Build(SizeKb);
        _utf8 = Encoding.UTF8.GetBytes(_json);
    }

    [Benchmark(Baseline = true, Description = "Deserialize<T>(string) [what GetString feeds]")]
    public int FromString()
    {
        return JsonSerializer.Deserialize<JsonPayload>(_json, _options)!.Items.Count;
    }

    [Benchmark(Description = "Deserialize<T>(ReadOnlySpan<byte>) [UTF-8, no transcode]")]
    public int FromUtf8Span()
    {
        return JsonSerializer.Deserialize<JsonPayload>(_utf8.AsSpan(), _options)!.Items.Count;
    }

    [Benchmark(Description = "Deserialize<T>(Stream) [what GetStream feeds]")]
    public int FromStream()
    {
        using var stream = new MemoryStream(_utf8, false);
        return JsonSerializer.Deserialize<JsonPayload>(stream, _options)!.Items.Count;
    }

    [Benchmark(Description = "UTF8.GetBytes(string) + Deserialize(span) [the transcode, priced]")]
    public int TranscodeThenSpan()
    {
        var bytes = Encoding.UTF8.GetBytes(_json);
        return JsonSerializer.Deserialize<JsonPayload>(bytes.AsSpan(), _options)!.Items.Count;
    }
}

/// <summary>The document these benchmarks deserialize into, and the generator for its JSON.</summary>
public class JsonPayload
{
    public Guid Id { get; set; }
    public string? Name { get; set; }
    public string? Description { get; set; }
    public List<BenchmarkDocument.LineItem> Items { get; set; } = [];
    public Dictionary<string, string> Attributes { get; set; } = [];

    /// <summary>
    ///     Repeats the reference document's line items until the serialized form clears the target,
    ///     so both sizes have the same shape and differ only in how much of it there is.
    /// </summary>
    public static string Build(int sizeKb)
    {
        // Serialized with the serializer's own options (camelCase), because that is what a stored
        // document looks like. Round-tripping through different options would leave every property
        // unmatched and quietly benchmark parsing into an empty object.
        var serializer = new SystemTextJsonSerializer();
        var document = BenchmarkDocument.Create();
        var target = sizeKb * 1024;
        var seed = document.Items.ToArray();

        while (serializer.ToJson(document).Length < target)
        {
            document.Items.AddRange(seed);
        }

        return serializer.ToJson(document);
    }
}

/// <summary>
///     The rejected candidate: pull the column's characters through <c>GetTextReader</c> and
///     transcode them into a rented UTF-8 buffer, so no full-size <see cref="string" /> is
///     materialized. It works, and it is portable in a way <c>GetStream</c> is not — but the
///     transcode it performs is the same one STJ would have performed internally, so it saves the
///     string allocation and pays for a buffer instead. Kept because the benchmark row it backs is
///     the evidence for not doing this.
/// </summary>
internal readonly struct PooledUtf8: IDisposable
{
    private readonly byte[] _buffer;
    private readonly int _length;

    private PooledUtf8(byte[] buffer, int length)
    {
        _buffer = buffer;
        _length = length;
    }

    public ReadOnlySpan<byte> Span => _buffer.AsSpan(0, _length);

    public static PooledUtf8 ReadFrom(System.Data.Common.DbDataReader reader, int index)
    {
        using var text = reader.GetTextReader(index);

        var chars = ArrayPool<char>.Shared.Rent(8 * 1024);
        var bytes = ArrayPool<byte>.Shared.Rent(32 * 1024);
        var written = 0;

        var encoder = Encoding.UTF8.GetEncoder();
        try
        {
            while (true)
            {
                var read = text.Read(chars, 0, chars.Length);
                if (read == 0)
                {
                    break;
                }

                // Worst case 3 bytes per BMP char, 4 with a surrogate pair carried over.
                var needed = written + (read * 4);
                if (needed > bytes.Length)
                {
                    var grown = ArrayPool<byte>.Shared.Rent(Math.Max(needed, bytes.Length * 2));
                    Array.Copy(bytes, grown, written);
                    ArrayPool<byte>.Shared.Return(bytes);
                    bytes = grown;
                }

                written += encoder.GetBytes(chars.AsSpan(0, read), bytes.AsSpan(written), false);
            }

            written += encoder.GetBytes(ReadOnlySpan<char>.Empty, bytes.AsSpan(written), true);
        }
        finally
        {
            ArrayPool<char>.Shared.Return(chars);
        }

        return new PooledUtf8(bytes, written);
    }

    public void Dispose()
    {
        if (_buffer is not null)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
        }
    }
}
