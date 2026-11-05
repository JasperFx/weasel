#nullable enable
using System;
using System.Buffers;
using System.Data.Common;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
///     Adapts any <see cref="ISerializer" /> (the shared Weasel.Core serialization base) to the
///     dialect-neutral <see cref="IStorageSerializer" /> seam of the shared closed-shape storage
///     runtime (JasperFx/polecat#273). A store's default serializer (derived from
///     <see cref="SystemTextJsonSerializer" /> and declaring the seam) implements it natively and
///     never needs this; the adapter covers user-supplied serializers, deriving the seam-only
///     members (<c>ToCleanJson</c>, <c>WriteTo</c>, <c>WriteToParameter</c>, async reader reads)
///     from the shared <c>Weasel.Core.ISerializer</c> members every serializer already carries.
///     Lifted from Polecat's and Fisher's byte-identical per-store copies (weasel#555).
/// </summary>
public sealed class StorageSerializerAdapter : IStorageSerializer
{
    private readonly ISerializer _inner;

    private StorageSerializerAdapter(ISerializer inner)
    {
        _inner = inner;
    }

    /// <summary>
    ///     Returns the serializer itself when it already implements the seam, otherwise wraps it.
    /// </summary>
    public static IStorageSerializer For(ISerializer serializer)
    {
        return serializer as IStorageSerializer ?? new StorageSerializerAdapter(serializer);
    }

    public string ToJson(object? document)
    {
        return document is null ? "null" : _inner.ToJson(document);
    }

    /// <summary>
    ///     The STJ-only stores have no type-metadata pollution to strip; "clean" JSON is plain JSON.
    /// </summary>
    public string ToCleanJson(object? document) => ToJson(document);

    public void WriteTo(IBufferWriter<byte> writer, object? value)
    {
        var json = ToJson(value);
        writer.Write(Encoding.UTF8.GetBytes(json));
    }

    /// <summary>
    ///     The stores using this seam bind JSON as string parameter values throughout (SQL Server
    ///     2025's native JSON column type accepts nvarchar input; SQLite's json1 functions accept
    ///     TEXT), so this stays dialect-neutral.
    /// </summary>
    public void WriteToParameter(DbParameter parameter, object? value)
    {
        parameter.Value = value is null ? DBNull.Value : ToJson(value);
    }

    public T FromJson<T>(Stream stream) => _inner.FromJson<T>(stream);

    public object FromJson(Type type, Stream stream) => _inner.FromJson(type, stream);

    public T FromJson<T>(DbDataReader reader, int index) => _inner.FromJson<T>(reader, index);

    public object FromJson(Type type, DbDataReader reader, int index) => _inner.FromJson(type, reader, index);

    public ValueTask<T> FromJsonAsync<T>(Stream stream, CancellationToken cancellationToken = default)
        => _inner.FromJsonAsync<T>(stream, cancellationToken);

    public ValueTask<object> FromJsonAsync(Type type, Stream stream, CancellationToken cancellationToken = default)
        => _inner.FromJsonAsync(type, stream, cancellationToken);

    /// <summary>
    ///     Stream the column where the provider allows it, and otherwise hand the read back to the
    ///     inner serializer's own reader read — the member a user-supplied serializer implements,
    ///     and the one the synchronous overloads above already use. <c>GetStream</c> is not
    ///     universal: SqlClient refuses it on <c>nvarchar</c> and on SQL Server 2025's native
    ///     <c>json</c>, which is weasel#778. Which providers are which is discovered rather than
    ///     declared; see <see cref="JsonColumnStreaming" /> (weasel#565).
    /// </summary>
    public async ValueTask<T> FromJsonAsync<T>(DbDataReader reader, int index,
        CancellationToken cancellationToken = default)
    {
        var stream = await JsonColumnStreaming.TryGetStreamAsync(reader, index, cancellationToken)
            .ConfigureAwait(false);

        if (stream is null)
        {
            return _inner.FromJson<T>(reader, index);
        }

        await using (stream.ConfigureAwait(false))
        {
            return await _inner.FromJsonAsync<T>(stream, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc cref="FromJsonAsync{T}(DbDataReader,int,CancellationToken)" />
    public async ValueTask<object> FromJsonAsync(Type type, DbDataReader reader, int index,
        CancellationToken cancellationToken = default)
    {
        var stream = await JsonColumnStreaming.TryGetStreamAsync(reader, index, cancellationToken)
            .ConfigureAwait(false);

        if (stream is null)
        {
            return _inner.FromJson(type, reader, index);
        }

        await using (stream.ConfigureAwait(false))
        {
            return await _inner.FromJsonAsync(type, stream, cancellationToken).ConfigureAwait(false);
        }
    }
}
