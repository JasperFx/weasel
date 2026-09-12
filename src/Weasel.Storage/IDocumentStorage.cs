#nullable enable
using System.Data.Common;
using JasperFx.Events.Aggregation;
using JasperFx.MultiTenancy;
using Weasel.Core;
using Weasel.Core.SqlGeneration;

namespace Weasel.Storage;

/// <summary>
///     Dialect-neutral contract for one closed-shape document storage variant (query-only,
///     lightweight, identity-map, or dirty-tracking): identity/typing metadata, the delete
///     fragments, tenancy style, and the query-filtering entry points the shared runtime and
///     LINQ pipeline drive. Store-specific concerns (the LINQ member model, bulk loading,
///     dialect-typed select clauses) live on derived interfaces in the owning store.
/// </summary>
public interface IDocumentStorage: ISelectClause
{
    Type SourceType { get; }

    Type IdType { get; }

    bool UseOptimisticConcurrency { get; }
    IOperationFragment DeleteFragment { get; }
    IOperationFragment HardDeleteFragment { get; }
    IReadOnlyList<IDuplicatedField> DuplicatedFields { get; }
    DbObjectName TableName { get; }
    Type DocumentType { get; }

    TenancyStyle TenancyStyle { get; }
    Task TruncateDocumentStorageAsync(IStorageDatabase database, CancellationToken ct = default);

    ISqlFragment FilterDocuments(ISqlFragment query, IStorageSession session);

    ISqlFragment? DefaultWhereFragment();

    bool UseNumericRevisions { get; }

    object RawIdentityValue(object id);
}

/// <summary>
///     Adds the document-typed store/eject/version members and the operation factory methods
///     (update/insert/upsert/overwrite and their session-free projection variants) to
///     <see cref="IDocumentStorage"/>.
/// </summary>
public interface IDocumentStorage<T>: IDocumentStorage where T : notnull
{
    object IdentityFor(T document);

    Guid? VersionFor(T document, IStorageSession session);

    /// <summary>
    ///     The value of the document's mapped optimistic-concurrency member, or null when the
    ///     mapping carries no mapped version member. Lets a session seed the expected version for a
    ///     write's concurrency guard off the document itself, exactly as it already does for the
    ///     store's versioned marker interfaces — without a runtime type test on the storage.
    /// </summary>
    /// <remarks>
    ///     Defaults to null so this stays additive: a storage with no mapped version member, or a
    ///     store that has not adopted the seam, keeps its current behaviour. See weasel#590, raised
    ///     from marten#5372 — a member mapped with Marten's <c>Metadata.Version.MapTo(...)</c> was
    ///     invisible to the session, so the upsert bound DBNull into its version guard and reported
    ///     every cross-session write as a concurrency violation.
    /// </remarks>
    Guid? MappedVersionFor(T document) => null;

    /// <summary>
    ///     The value of the document's mapped numeric-revision member, or null when the mapping
    ///     carries no mapped revision member. Returns a long whatever the member's own width is —
    ///     an int member widens — because the revision column itself comes in both widths.
    /// </summary>
    long? MappedRevisionFor(T document) => null;

    void Store(IStorageSession session, T document);
    void Store(IStorageSession session, T document, Guid? version);
    void Store(IStorageSession session, T document, long revision);

    void Eject(IStorageSession session, T document);

    IStorageOperation Update(T document, IStorageSession session, string tenantId);
    IStorageOperation Insert(T document, IStorageSession session, string tenantId);
    IStorageOperation Upsert(T document, IStorageSession session, string tenantId);

    IStorageOperation Overwrite(T document, IStorageSession session, string tenantId);

    /// <summary>
    ///     Lighter-weight overwrite for projection storage. Builds the same Overwrite operation
    ///     but does NOT consult session-level version / revision tracking, so it is safe to call
    ///     from parallel async-daemon slice handlers that share a session (the session is, by
    ///     contract, not thread-safe; projections set the revision explicitly from the event and
    ///     never read the session's version tracker back).
    /// </summary>
    IStorageOperation OverwriteProjected(T document, string tenantId);

    /// <summary>
    ///     Session-free Upsert for projection storage. Builds the same Upsert operation as
    ///     <see cref="Upsert"/> but passes a null version/revision tracker so the projection path
    ///     never touches the session's version tracking. Safe to call from parallel async-daemon
    ///     slice handlers that share a session.
    /// </summary>
    IStorageOperation UpsertProjected(T document, string tenantId);

    /// <summary>
    ///     Session-free Insert for projection storage. See <see cref="UpsertProjected"/>.
    /// </summary>
    IStorageOperation InsertProjected(T document, string tenantId);

    /// <summary>
    ///     Session-free Update for projection storage. See <see cref="UpsertProjected"/>.
    /// </summary>
    IStorageOperation UpdateProjected(T document, string tenantId);

    IDeletion DeleteForDocument(T document, string tenantId);

    void EjectById(IStorageSession session, object id);
    void RemoveDirtyTracker(IStorageSession session, object id);
    IDeletion HardDeleteForDocument(T document, string tenantId);

    void SetIdentityFromString(T document, string identityString);
    void SetIdentityFromGuid(T document, Guid identityGuid);
}

/// <summary>
///     The identity-typed closed shape: id-keyed load / delete / filter members plus the
///     session-free projection load path (fresh connection off <see cref="IStorageDatabase"/>,
///     no session-shared state).
/// </summary>
public interface IDocumentStorage<T, TId>: IDocumentStorage<T>, IIdentitySetter<T, TId> where T : notnull where TId : notnull
{
    IDeletion DeleteForId(TId id, string tenantId);

    Task<T?> LoadAsync(TId id, IStorageSession session, CancellationToken token);

    Task<IReadOnlyList<T>> LoadManyAsync(TId[] ids, IStorageSession session, CancellationToken token);

    /// <summary>
    ///     Session-free Load for projection storage. Opens a fresh connection from the database,
    ///     executes the load SQL, and returns the deserialized document. Does not touch any
    ///     session-shared state — no version/revision tracker writes, no ItemMap updates, no
    ///     MarkAsDocumentLoaded, no ChangeTrackers writes. Safe to call from parallel
    ///     async-daemon slice handlers that share a session.
    /// </summary>
    Task<T?> LoadProjectedAsync(TId id, IStorageDatabase database, string tenantId, CancellationToken token);

    /// <summary>
    ///     Session-free LoadMany for projection storage. See <see cref="LoadProjectedAsync"/>.
    /// </summary>
    Task<IReadOnlyList<T>> LoadManyProjectedAsync(TId[] ids, IStorageDatabase database, string tenantId, CancellationToken token);

    TId AssignIdentity(T document, string tenantId, IStorageDatabase database);
    ISqlFragment ByIdFilter(TId id);
    IDeletion HardDeleteForId(TId id, string tenantId);
    DbCommand BuildLoadCommand(TId id, string tenantId);
    DbCommand BuildLoadManyCommand(TId[] ids, string tenantId);
    object RawIdentityValue(TId id);
}
