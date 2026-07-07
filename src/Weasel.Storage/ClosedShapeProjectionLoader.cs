#nullable enable
using System.Data.Common;

namespace Weasel.Storage;

/// <summary>
/// Shared projection-safe load implementation for closed-shape document storages. Opens a
/// fresh connection from the <see cref="IStorageDatabase"/>, executes the load SQL, and
/// deserializes the data column directly via the <see cref="IStorageSerializer"/> — without
/// any session reference, without writing to the version tracker / identity map / change
/// trackers, and without marking documents as loaded.
/// </summary>
/// <remarks>
/// <para>
/// The projection-safe selector path intentionally skips metadata binders (CreatedAt /
/// LastModified / Headers / etc.). Projections care about the aggregate state encoded in the
/// data column for their Apply/Evolve hot path; per-row metadata is not part of that contract.
/// If a future projection scenario needs metadata it can be added here as a focused follow-up.
/// </para>
/// <para>
/// Hierarchical storages dispatch deserialization through the descriptor's
/// <see cref="DocumentStorageDescriptor{TDoc,TId}.ResolveDocumentType"/> alias-to-.NET-type
/// lookup, mirroring the hierarchical query-only selector.
/// </para>
/// </remarks>
public static class ClosedShapeProjectionLoader<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    // Column layout matches the writeable closed-shape selectors (Lightweight / IdentityMap /
    // DirtyChecked) since LoadProjectedAsync is only reached from those storages. QueryOnly
    // storage has a different layout (id excluded, data at col 0) but doesn't implement
    // LoadProjectedAsync; it throws NotSupportedException instead.
    private const int IdColumn = 0;
    private const int DataColumn = 1;
    private const int FirstMetadataColumn = 2;

    public static async Task<TDoc?> LoadAsync(
        DbCommand command,
        DocumentStorageDescriptor<TDoc, TId> descriptor,
        IStorageSerializer serializer,
        IStorageDatabase database,
        CancellationToken token)
    {
        await using var conn = database.CreateStorageConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);
        try
        {
            command.Connection = conn;
            await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
            if (!await reader.ReadAsync(token).ConfigureAwait(false))
            {
                return default;
            }

            return await readOneAsync(reader, descriptor, serializer, token).ConfigureAwait(false);
        }
        finally
        {
            await conn.CloseAsync().ConfigureAwait(false);
        }
    }

    public static async Task<IReadOnlyList<TDoc>> LoadManyAsync(
        DbCommand command,
        DocumentStorageDescriptor<TDoc, TId> descriptor,
        IStorageSerializer serializer,
        IStorageDatabase database,
        CancellationToken token)
    {
        await using var conn = database.CreateStorageConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);
        try
        {
            command.Connection = conn;
            await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
            var list = new List<TDoc>();
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                var doc = await readOneAsync(reader, descriptor, serializer, token).ConfigureAwait(false);
                if (doc is not null)
                {
                    list.Add(doc);
                }
            }
            return list;
        }
        finally
        {
            await conn.CloseAsync().ConfigureAwait(false);
        }
    }

    private static async ValueTask<TDoc> readOneAsync(
        DbDataReader reader,
        DocumentStorageDescriptor<TDoc, TId> descriptor,
        IStorageSerializer serializer,
        CancellationToken token)
    {
        // Hierarchical: dispatch via the doc-type alias just like the hierarchical query-only
        // selector. Flat: straight deserialize.
        if (descriptor.ResolveDocumentType is { } resolveType)
        {
            var docTypeOrdinal = FirstMetadataColumn + descriptor.DocTypeReadIndex;
            var alias = await reader.GetFieldValueAsync<string>(docTypeOrdinal, token).ConfigureAwait(false);
            return (TDoc)await serializer.FromJsonAsync(resolveType(alias), reader, DataColumn, token).ConfigureAwait(false);
        }

        return await serializer.FromJsonAsync<TDoc>(reader, DataColumn, token).ConfigureAwait(false);
    }
}
