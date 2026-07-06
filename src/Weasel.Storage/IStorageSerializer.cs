#nullable enable
using System;
using System.Buffers;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Weasel.Storage;

/// <summary>
///     Dialect-neutral serializer seam consumed by the shared document-storage runtime. Exposes only
///     the JSON read/write members the storage / binders / selectors use, carrying no provider-typed
///     members — the one write hook that binds a parameter is surfaced against the neutral
///     <see cref="DbParameter"/>. The concrete implementation lives in the consuming library (e.g.
///     Marten's serializer implements this over its provider's parameter type).
/// </summary>
public interface IStorageSerializer
{
    /// <summary>Serialize the document object into a JSON string.</summary>
    string ToJson(object? document);

    /// <summary>
    ///     Serialize <paramref name="value"/> directly into the supplied buffer writer as UTF-8 JSON —
    ///     the allocation-free append/write hot path.
    /// </summary>
    void WriteTo(IBufferWriter<byte> writer, object? value);

    /// <summary>Serialize a document without any extra type-handling metadata.</summary>
    string ToCleanJson(object? document);

    /// <summary>
    ///     Serialize <paramref name="value"/> as UTF-8 JSON and bind it to the supplied parameter as
    ///     JSON/JSONB. The concrete implementation binds against its provider's parameter type.
    /// </summary>
    void WriteToParameter(DbParameter parameter, object? value);

    /// <summary>Deserialize a JSON stream into an object of type T.</summary>
    T FromJson<T>(Stream stream);

    /// <summary>Deserialize the JSON at the reader's column index into an object of type T.</summary>
    T FromJson<T>(DbDataReader reader, int index);

    /// <summary>Deserialize a JSON stream into the supplied Type.</summary>
    object FromJson(Type type, Stream stream);

    /// <summary>Deserialize the JSON at the reader's column index into the supplied Type.</summary>
    object FromJson(Type type, DbDataReader reader, int index);

    /// <summary>Deserialize a JSON stream into an object of type T.</summary>
    ValueTask<T> FromJsonAsync<T>(Stream stream, CancellationToken cancellationToken = default);

    /// <summary>Deserialize the JSON at the reader's column index into an object of type T.</summary>
    ValueTask<T> FromJsonAsync<T>(DbDataReader reader, int index, CancellationToken cancellationToken = default);

    /// <summary>Deserialize a JSON stream into the supplied Type.</summary>
    ValueTask<object> FromJsonAsync(Type type, Stream stream, CancellationToken cancellationToken = default);

    /// <summary>Deserialize the JSON at the reader's column index into the supplied Type.</summary>
    ValueTask<object> FromJsonAsync(Type type, DbDataReader reader, int index,
        CancellationToken cancellationToken = default);
}
