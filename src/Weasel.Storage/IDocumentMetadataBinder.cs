#nullable enable
using System.Data.Common;

namespace Weasel.Storage;

/// <summary>
/// Per-metadata-column seam for the closed-shape document storage runtime — every metadata
/// column on the document table (version, .NET type, last-modified, tenant id, soft-delete
/// flag, custom user-set metadata, etc.) gets one binder. The
/// <see cref="DocumentStorageDescriptor{TDoc,TId}"/> holds them in ordered arrays; the storage
/// operations loop over the client-side subset in <c>ConfigureCommand</c> and the selectors
/// loop over the full array on read.
/// </summary>
/// <remarks>
/// <para>
/// Per-call cost is one virtual <see cref="BindParameter"/> per active client-side metadata
/// column on the write path. Server-side values (e.g. <c>transaction_timestamp()</c>) declare a
/// non-<c>?</c> <see cref="ValueSql"/> fragment that goes directly into the VALUES list — they
/// have no parameter slot to fill and <see cref="IsServerSide"/> is <c>true</c>.
/// </para>
/// </remarks>
public interface IDocumentMetadataBinder<TDoc>
    where TDoc : notnull
{
    /// <summary>
    /// Stable column name used to compose the INSERT column list, the SELECT projection, and
    /// the ON CONFLICT UPDATE clause. The descriptor builder threads these through to the SQL
    /// strings in the same order as the binder array — parameter order matches column order.
    /// </summary>
    string ColumnName { get; }

    /// <summary>
    /// SQL fragment placed in the VALUES list at this binder's position. <c>"?"</c> for a bound
    /// parameter (the common case); <c>"transaction_timestamp()"</c> / <c>"now()"</c> / similar
    /// for a server-side computed value.
    /// </summary>
    string ValueSql { get; }

    /// <summary>
    /// True when <see cref="ValueSql"/> is something other than <c>"?"</c> — i.e. the value is
    /// computed server-side and this binder has no parameter slot to fill on write. Server-side
    /// binders skip <see cref="BindParameter"/> but may still implement <see cref="Apply"/> to
    /// project the column's value back onto the document on read.
    /// </summary>
    bool IsServerSide => ValueSql != "?";

    /// <summary>
    /// Per-row write hook on the INSERT / UPSERT path. The storage operation pre-allocates the
    /// parameter from the SQL's <c>?</c> placeholders and hands it to each client-side binder in
    /// order. Server-side binders don't get called here — their <see cref="ValueSql"/> fragment
    /// is in the SQL directly. The parameter is the db-neutral <see cref="DbParameter"/>; the
    /// concrete dialect binders set the provider type on the underlying parameter.
    /// </summary>
    void BindParameter(DbParameter parameter, TDoc document, IStorageSession session);

    /// <summary>
    /// Optional pre-serialization hook. Called once per write before the document body is
    /// serialized into the <c>data</c> parameter — gives session-derived binders (Correlation/
    /// Causation/LastModifiedBy/Headers) a chance to project the session value onto the
    /// document's mapped member so the value flows into the JSON data column too. Default
    /// no-op; binders whose value isn't session-derived (Version, LastModified, soft-delete,
    /// etc.) don't override.
    /// </summary>
    void ApplyToDocument(TDoc document, IStorageSession session) { }

    /// <summary>
    /// Per-row read hook on the SELECT path. Called from the selector after the document body
    /// has been deserialized, with the metadata column's position in the result row. Binders
    /// for columns whose values get projected onto the document (Version → version member,
    /// LastModified → last-modified member) write through to the doc's annotated member;
    /// binders whose stored values are purely informational no-op. The
    /// <paramref name="session"/> is threaded through for binders that need to deserialize JSON
    /// values (e.g. Headers) via the configured <see cref="IStorageSerializer"/>.
    /// </summary>
    void Apply(DbDataReader reader, int columnOrdinal, TDoc document, IStorageSession session);

    /// <summary>
    /// The binder's contribution to a bulk-load row, as a dialect-neutral
    /// <see cref="BulkColumnValue"/> (value + <see cref="StorageColumnType"/>). The
    /// dialect-specific bulk loader maps the type to its provider and writes. Server-side
    /// binders (e.g. <c>transaction_timestamp()</c>) compute a client-side value here since
    /// bulk-copy protocols don't honor inline SQL literals. Any document mutation the value
    /// implies (version writeback, LastModified) happens as a side effect of this call.
    /// </summary>
    BulkColumnValue GetBulkValue(TDoc document);
}

/// <summary>
/// The version (e.g. <c>mt_version</c> Guid) metadata binder, surfaced on the
/// <see cref="DocumentStorageDescriptor{TDoc,TId}"/> so the optimistic-concurrency operations
/// can write the new version back onto the document from <c>Postprocess</c> without re-walking
/// the read binders. The owning store's concrete binder implements this.
/// </summary>
public interface IVersionMetadataBinder<TDoc>: IDocumentMetadataBinder<TDoc>
    where TDoc : notnull
{
    void ApplyVersionTo(TDoc document, Guid version);
}

/// <summary>
/// The numeric-revision metadata binder (monotonic int/bigint revisions), surfaced on the
/// <see cref="DocumentStorageDescriptor{TDoc,TId}"/> so the numeric-concurrency operations can
/// write the new revision back onto the document and bind the revision parameter with the
/// right column type. The owning store's concrete binder implements this.
/// </summary>
public interface IRevisionMetadataBinder<TDoc>: IDocumentMetadataBinder<TDoc>
    where TDoc : notnull
{
    StorageColumnType RevisionColumnType { get; }

    void ApplyRevisionTo(TDoc document, long revision);
}
