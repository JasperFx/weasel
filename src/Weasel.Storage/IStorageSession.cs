#nullable enable
using System.Data.Common;
using JasperFx;

namespace Weasel.Storage;

/// <summary>
///     Dialect-neutral operation/session context the closed-shape document storage, operation,
///     and selector runtime targets — the gating seam for sharing that runtime across stores
///     (JasperFx/marten#4810 / #4821). The unit-of-work metadata (tenant id / correlation /
///     causation / user name / headers) comes from the <see cref="IMetadataContext"/> base.
/// </summary>
/// <remarks>
///     Deliberately exposes only the subset of members the closed-shape document + event storage
///     code actually consumes: the serializer/database/version seams, the identity-map and
///     dirty-tracking state, and a database-neutral command execution entry point.
/// </remarks>
public interface IStorageSession: IMetadataContext
{
    IStorageSerializer Serializer { get; }

    IStorageDatabase Database { get; }

    IVersionTracker Versions { get; }

    IList<IChangeTracker> ChangeTrackers { get; }

    Dictionary<Type, object> ItemMap { get; }

    /// <summary>
    ///     Override whether or not this session honors optimistic concurrency checks
    /// </summary>
    ConcurrencyChecks Concurrency { get; }

    /// <summary>
    ///     Opt in to <see cref="ChangeTracker{T}"/>'s semantic fallback: when a tracked document's
    ///     JSON text differs from its load-time text, parse both and compare the trees before
    ///     concluding that the document changed. Default <c>false</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     The fallback exists for documents whose JSON text can reorder without changing meaning.
    ///     A <c>Dictionary</c> or <c>HashSet</c> member serializes in enumeration order, and
    ///     enumeration order follows the collection's insertion and removal history rather than
    ///     its contents — so a dictionary that is rebuilt, or has an entry removed and re-added,
    ///     serializes to different text for the same document under one deterministic serializer
    ///     and no custom converter. STJ's <c>[JsonExtensionData]</c> is backed by a dictionary and
    ///     behaves the same way.
    ///     </para>
    ///     <para>
    ///     It is off by default because it costs roughly 8x the time and 10x the allocation of an
    ///     ordinary unchanged check (weasel#577). Turning it on buys exactly one thing: not
    ///     issuing a redundant, semantically identical update when a collection reorders. It can
    ///     never buy a write back, because the check it performs only ever turns "changed" into
    ///     "unchanged" — so a session that leaves it off risks a spurious write, never a lost one.
    ///     </para>
    /// </remarks>
    bool UseSemanticJsonChangeDetection => false;

    IDocumentStorage StorageFor(Type documentType);

    IDocumentStorage<T> StorageFor<T>() where T : notnull;

    void MarkAsAddedForStorage(object id, object document);

    void MarkAsDocumentLoaded(object id, object document);

    /// <summary>
    ///     Execute a single command against this session's connection and return the results.
    ///     Database-neutral execution seam: the closed-shape read path (document LoadAsync /
    ///     LoadManyAsync) targets <see cref="DbCommand"/> here instead of a dialect-typed
    ///     command. The owning store's session implements this over its connection lifetime.
    /// </summary>
    Task<DbDataReader> ExecuteReaderAsync(DbCommand command, CancellationToken token = default);

    /// <summary>
    ///     JSON-encoded bytes of the session's current header dictionary, when the owning store's
    ///     session caches that serialization per batch — the headers metadata binder uses it to
    ///     avoid re-serializing the same dictionary for every queued operation. Returns null when
    ///     there are no headers (callers then bind a typed null). Default: no cache.
    /// </summary>
    byte[]? TryGetCachedSerializedHeaders() => null;

    /// <summary>
    ///     Generates a unique temporary-table / CTE name scoped to this session. Used by LINQ
    ///     statement compilers for include queries and chained sub-selects — a session-scoped
    ///     concern any dialect performing include/CTE queries needs.
    /// </summary>
    string NextTempTableName();
}
