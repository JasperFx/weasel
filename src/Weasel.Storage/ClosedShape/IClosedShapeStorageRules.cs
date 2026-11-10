#nullable enable
using JasperFx.MultiTenancy;
using Weasel.Core;
using Weasel.Core.SqlGeneration;

namespace Weasel.Storage;

/// <summary>
///     The four document-tracking modes a closed-shape storage family is built in. Each mode is one
///     slot of <see cref="DocumentProvider{T}" />.
/// </summary>
public enum ClosedShapeTrackingMode
{
    /// <summary>Read-only: no identity map, no version capture, writes refused.</summary>
    QueryOnly,

    /// <summary>No identity map; versions are captured for optimistic / numeric guards.</summary>
    Lightweight,

    /// <summary>Loaded and stored documents are kept in the session's identity map.</summary>
    IdentityMap,

    /// <summary>Identity map plus a change tracker per loaded document.</summary>
    DirtyTracking
}

/// <summary>
///     The SQL a closed-shape storage reads with in one <see cref="ClosedShapeTrackingMode" />. The
///     column order is a contract with the shared selectors: the query-only selectors read the
///     document body at column 0 with no id, the writeable modes read the id at 0 and the body at 1,
///     metadata after that.
/// </summary>
/// <param name="SelectFields">The selected column expressions, in read order.</param>
/// <param name="SelectClause">The full <c>select … from …</c> clause written by <c>Apply</c>.</param>
/// <param name="LoaderSql">The single-document load statement handed to <see cref="IStorageDialect.BuildLoadCommand" />.</param>
/// <param name="LoadArraySql">The load-many statement handed to <see cref="IStorageDialect.BuildLoadManyCommand" />.</param>
public sealed record ClosedShapeReadSql(string[] SelectFields, string SelectClause, string LoaderSql, string LoadArraySql);

/// <summary>
///     Everything about one document type's closed-shape storage that differs from store to store,
///     supplied by the owning store (Marten, Polecat, Fisher) to
///     <see cref="ClosedShapeDocumentProviderFactory" />. The storages Weasel builds hold this object and
///     delegate to it; the descriptor carries the rest.
/// </summary>
/// <remarks>
///     <para>
///         <b>A store's own storage interfaces live here, not on the storage.</b> The storage classes are
///         Weasel's and are not open for derivation, so a store that needs more from a storage than
///         <see cref="IDocumentStorage{T,TId}" /> offers (LINQ handlers, batch-load SQL, bulk-insert
///         hooks) implements that interface on its rules object and reaches it through
///         <see cref="IClosedShapeDocumentStorage.Rules" />.
///     </para>
///     <para>
///         <b>One rules object serves all four storages of a document type.</b>
///         <see cref="ClosedShapeDocumentProviderFactory.Build{TDoc,TId}" /> hands the same instance to
///         every tracking mode, so the members that take a storage are called once per storage with a
///         different one each time. Do not cache the storage argument, and give the store's own
///         interfaces on this object a storage parameter rather than a remembered storage.
///     </para>
///     <para>
///         Members with a default body are optional. Weasel does not ship breaking changes, so any member
///         added here later will carry a default body too.
///     </para>
/// </remarks>
public interface IClosedShapeStorageRules<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    /// <summary>The document table.</summary>
    DbObjectName TableName { get; }

    TenancyStyle TenancyStyle { get; }

    IReadOnlyList<IDuplicatedField> DuplicatedFields { get; }

    /// <summary>The read SQL for one tracking mode. Called once per storage, at construction.</summary>
    ClosedShapeReadSql ReadSqlFor(ClosedShapeTrackingMode mode);

    /// <summary>
    ///     The fragment a delete writes: a hard <c>DELETE</c>, or the soft-delete <c>UPDATE</c> when the
    ///     document type is soft-deleted. Called once per storage, at construction.
    /// </summary>
    IOperationFragment BuildDeleteFragment(IDocumentStorage<TDoc, TId> storage);

    /// <summary>The fragment a hard delete writes. Called once per storage, at construction.</summary>
    IOperationFragment BuildHardDeleteFragment(IDocumentStorage<TDoc, TId> storage);

    /// <summary>
    ///     Build the deletion for one id. <paramref name="byIdFilter" /> is the dialect's id filter;
    ///     the store adds its tenant filter for a conjoined document type.
    /// </summary>
    IDeletion BuildDeletion(IDocumentStorage<TDoc, TId> storage, IOperationFragment fragment, TId id,
        ISqlFragment byIdFilter, string tenantId);

    /// <summary>Add the soft-delete and tenant filters a query against this document type needs.</summary>
    ISqlFragment FilterDocuments(ISqlFragment query, IStorageSession session);

    /// <summary>The filters every query against this document type carries by default, or null.</summary>
    ISqlFragment? DefaultWhereFragment();

    /// <summary>The statement that empties the document table, e.g. <c>truncate … cascade</c>.</summary>
    string TruncateSql { get; }

    /// <summary>Write the id onto the document.</summary>
    void SetIdentity(TDoc document, TId identity);

    /// <summary>Write an id given as a string onto the document. Default: not supported.</summary>
    void SetIdentityFromString(TDoc document, string identityString)
        => throw new NotSupportedException(
            $"{typeof(TDoc).Name} does not take its identity from a string.");

    /// <summary>Write an id given as a Guid onto the document. Default: not supported.</summary>
    void SetIdentityFromGuid(TDoc document, Guid identityGuid)
        => throw new NotSupportedException(
            $"{typeof(TDoc).Name} does not take its identity from a Guid.");

    /// <summary>The document's mapped optimistic-version member, if it has one. Default: none.</summary>
    Guid? MappedVersionFor(TDoc document) => null;

    /// <summary>The document's mapped numeric-revision member, if it has one. Default: none.</summary>
    long? MappedRevisionFor(TDoc document) => null;

    /// <summary>
    ///     The descriptor a write operation is built over. Default: the storage's own. A store whose
    ///     write SQL depends on the session or the document (an unguarded variant under a session that
    ///     ignores concurrency, say) returns a different descriptor here.
    /// </summary>
    /// <param name="role">Insert, Update, Upsert, or Other for an overwrite.</param>
    /// <param name="session">The writing session, or null on the session-free projection paths.</param>
    DocumentStorageDescriptor<TDoc, TId> WriteDescriptorFor(OperationRole role, TDoc document,
        IStorageSession? session, DocumentStorageDescriptor<TDoc, TId> descriptor)
        => descriptor;

    /// <summary>
    ///     True for a session shared by parallel projection workers, such as the async daemon's. A
    ///     Lightweight storage hands such a session a selector that captures no versions, because
    ///     capturing them writes to session-shared dictionaries from several threads. Default: false.
    /// </summary>
    bool IsSharedProjectionSession(IStorageSession session) => false;

    /// <summary>
    ///     Point the nested session's version entries for this document type at the parent session's,
    ///     as the identity map already is. Only the store knows its version tracker's layout, so the
    ///     default does nothing and a nested session keeps versions of its own.
    /// </summary>
    void ShareVersions(IStorageSession parent, IStorageSession nested)
    {
    }

    /// <summary>
    ///     Non-null when the store has no numeric revisions: <c>Store(document, revision)</c> then
    ///     throws <see cref="NotSupportedException" /> with this message. Default: null, supported.
    /// </summary>
    string? NumericRevisionsNotSupportedReason => null;
}
