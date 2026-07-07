#nullable enable
namespace Weasel.Storage;

/// <summary>
/// Which concurrency model the document mapping uses. Drives WHERE-clause additions on
/// UPDATE/UPSERT, RETURNING column choice, and the Postprocess exception type. Encoded once on
/// the descriptor so the operation classes can switch on it without reading mapping state per
/// call.
/// </summary>
public enum ConcurrencyMode
{
    /// <summary>No optimistic concurrency. Updates use plain WHERE id = ?.</summary>
    Off,

    /// <summary>
    /// Guid-based optimistic concurrency. Each write generates a fresh Guid version; UPDATE /
    /// UPSERT add <c>and mt_version = ?</c> to the predicate and RETURN the new version for
    /// postprocess validation. A miss (no row returned) raises a concurrency exception instead
    /// of a missing-document exception.
    /// </summary>
    Optimistic,

    /// <summary>
    /// Monotonic-bigint revisions. Each write either auto-increments (caller passes
    /// <c>Revision = 0</c>) or supplies an explicit revision that must be strictly greater than
    /// the current row's version column.
    /// </summary>
    Numeric
}

/// <summary>
/// Per-mapping descriptor that the closed-shape document storage class composes with at
/// construction. Holds the pre-built SQL strings + the ordered metadata-binder arrays. Built
/// once per document mapping by the owning store's descriptor builder; held as a
/// <c>readonly</c> field on the storage instance.
/// </summary>
public sealed class DocumentStorageDescriptor<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public DocumentStorageDescriptor(
        Core.Identity.IIdentification<TDoc, TId> identification,
        IStorageSerializer serializer,
        IStorageDialect dialect,
        IDocumentMetadataBinder<TDoc>[] clientSideWriteBinders,
        IDocumentMetadataBinder<TDoc>[] writeBinders,
        IDocumentMetadataBinder<TDoc>[] readBinders,
        IDocumentMetadataBinder<TDoc>[] queryOnlyReadBinders,
        string upsertSql,
        string insertSql,
        string updateSql,
        string overwriteSql,
        bool isConjoined,
        ConcurrencyMode concurrencyMode,
        IVersionMetadataBinder<TDoc>? versionBinder,
        IRevisionMetadataBinder<TDoc>? revisionBinder,
        int versionReadIndex,
        Func<string, Type>? resolveDocumentType,
        int docTypeReadIndex,
        string tableName,
        IDocumentMetadataBinder<TDoc>[]? partitionPkBinders = null,
        bool useVersionFromMatchingStream = false,
        IOperationExceptionTransform? exceptionTransform = null,
        Func<Type, object, Exception>? createMissingDocumentException = null)
    {
        Identification = identification;
        Serializer = serializer;
        Dialect = dialect;
        ClientSideWriteBinders = clientSideWriteBinders;
        WriteBinders = writeBinders;
        ReadBinders = readBinders;
        QueryOnlyReadBinders = queryOnlyReadBinders;
        TableName = tableName;
        UpsertSql = upsertSql;
        InsertSql = insertSql;
        UpdateSql = updateSql;
        OverwriteSql = overwriteSql;
        IsConjoined = isConjoined;
        ConcurrencyMode = concurrencyMode;
        VersionBinder = versionBinder;
        RevisionBinder = revisionBinder;
        VersionReadIndex = versionReadIndex;
        ResolveDocumentType = resolveDocumentType;
        DocTypeReadIndex = docTypeReadIndex;
        PartitionPkBinders = partitionPkBinders ?? Array.Empty<IDocumentMetadataBinder<TDoc>>();
        UseVersionFromMatchingStream = useVersionFromMatchingStream;
        ExceptionTransform = exceptionTransform;
        CreateMissingDocumentException = createMissingDocumentException;
    }

    /// <summary>
    ///     Optional store-supplied translation of provider exceptions raised by the write
    ///     operations (unique-constraint violations, etc.). Null = no translation.
    /// </summary>
    public IOperationExceptionTransform? ExceptionTransform { get; }

    /// <summary>
    ///     Optional store-supplied factory for the exception raised when an Update targets a
    ///     document that does not exist. Null = a plain InvalidOperationException.
    /// </summary>
    public Func<Type, object, Exception>? CreateMissingDocumentException { get; }

    /// <summary>
    /// When set, the closed-shape SQL pulls the revision from the matching row in the event
    /// store's streams table (used by inline single-stream projections under quick event
    /// append, where the in-memory event version hasn't been assigned yet). The Insert / Upsert
    /// operations consult this flag to bind extra ? slots for the stream-lookup subquery's id
    /// (and tenant_id under conjoined tenancy).
    /// </summary>
    public bool UseVersionFromMatchingStream { get; }

    /// <summary>
    /// Writers that bind the partition PK columns. For Update/Upsert on partitioned tables
    /// whose PK includes the partition column (e.g. list-partitioned by a soft-delete flag,
    /// range-partitioned by a duplicated date field) the WHERE clause needs to filter on those
    /// columns too — otherwise UPDATE … WHERE id = ? targets every partition row with that id
    /// and produces a PK violation when the new value moves it back into another row's slot.
    /// Order matches the SQL emit.
    /// </summary>
    public IDocumentMetadataBinder<TDoc>[] PartitionPkBinders { get; }

    public Core.Identity.IIdentification<TDoc, TId> Identification { get; }

    /// <summary>
    /// The store-global serializer, captured on the descriptor so the projection read path
    /// (<c>LoadProjectedAsync</c>/<c>LoadManyProjectedAsync</c>) needs no store mapping
    /// reference.
    /// </summary>
    public IStorageSerializer Serializer { get; }

    /// <summary>
    /// The ADO/SQL-dialect strategy for this document type. The owning store's builder supplies
    /// its dialect singleton; the closed-shape storage classes expose it so the shared runtime
    /// holds no direct provider reference.
    /// </summary>
    public IStorageDialect Dialect { get; }

    /// <summary>
    /// Subset of the metadata binders that consume a <c>?</c> parameter slot in the VALUES
    /// list. Server-side binders (e.g. <c>transaction_timestamp()</c> for last-modified) are
    /// excluded — their literal SQL is baked into <see cref="UpsertSql"/>.
    /// </summary>
    public IDocumentMetadataBinder<TDoc>[] ClientSideWriteBinders { get; }

    /// <summary>
    /// All write binders — client-side <em>and</em> server-side. The bulk-copy path uses this
    /// because binary copy protocols can't run inline SQL literals, so each binder writes a
    /// client-computed value via <see cref="IDocumentMetadataBinder{TDoc}.GetBulkValue"/> —
    /// including LastModified, which computes UtcNow instead of emitting
    /// <c>transaction_timestamp()</c>.
    /// </summary>
    public IDocumentMetadataBinder<TDoc>[] WriteBinders { get; }

    /// <summary>
    /// Unqualified table name (without schema prefix) — used by the exception-transform path to
    /// detect when a unique-constraint violation belongs to this document type's table so it
    /// can be surfaced as <see cref="JasperFx.DocumentAlreadyExistsException"/>.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// All metadata binders applied on the read path, in column order. Includes both
    /// client-side and server-side binders — on read they're symmetric (each binder consumes
    /// one result column).
    /// </summary>
    public IDocumentMetadataBinder<TDoc>[] ReadBinders { get; }

    /// <summary>
    /// The read-binder set for the QueryOnly storage style. Identical to
    /// <see cref="ReadBinders"/> except it omits the version/revision binder when that column
    /// has no annotated member — the version column is absent from the QueryOnly SELECT in that
    /// case and its binders would otherwise be offset by one. Same array instance as
    /// <see cref="ReadBinders"/> when nothing is dropped.
    /// </summary>
    public IDocumentMetadataBinder<TDoc>[] QueryOnlyReadBinders { get; }

    /// <summary>
    /// Full upsert SQL with <c>?</c> placeholders for client-side parameters and inline
    /// literals for server-side ones. Fed to the command builder's placeholder-expanding append
    /// at write time; the returned <see cref="System.Data.Common.DbParameter"/> array is filled
    /// by the operation in order: id, data, then each <see cref="ClientSideWriteBinders"/>
    /// entry.
    /// </summary>
    public string UpsertSql { get; }

    /// <summary>
    /// SQL for the Insert path —
    /// <c>"insert into … (id, data, …) values (?, ?, …) on conflict (id) do nothing returning id"</c>.
    /// The trailing RETURNING is consumed by the insert operation's Postprocess so a missing
    /// row (conflict) raises a document-already-exists exception. Parameter order matches
    /// <see cref="UpsertSql"/>: id, data, then each client-side binder.
    /// </summary>
    public string InsertSql { get; }

    /// <summary>
    /// SQL for the Update path —
    /// <c>"update … set data = ?, mt_version = ?, … where id = ? returning id"</c>.
    /// Parameter order: data first, then each client-side binder, then id (the WHERE clause).
    /// Postprocess raises a missing-document exception when no row comes back.
    /// </summary>
    public string UpdateSql { get; }

    /// <summary>
    /// SQL for the Overwrite path — identical to <see cref="UpsertSql"/> when
    /// <see cref="ConcurrencyMode"/> is <c>Off</c>; under optimistic concurrency the trailing
    /// WHERE filter on the version column is stripped so the write always wins.
    /// </summary>
    public string OverwriteSql { get; }

    /// <summary>
    /// When <c>true</c>, the document table is conjoined-multi-tenanted: <c>tenant_id</c> is
    /// part of the primary key, INSERT carries it as the first parameter, UPDATE has
    /// <c>and tenant_id = ?</c> appended to the WHERE clause, and ON CONFLICT references
    /// <c>(tenant_id, id)</c>. The operations bind the tenant id directly from the tenant
    /// argument the storage class receives.
    /// </summary>
    public bool IsConjoined { get; }

    /// <summary>
    /// Which concurrency model the mapping uses. <see cref="UpsertSql"/> / <see cref="UpdateSql"/>
    /// are already baked for the selected mode; operation classes only read this property to
    /// decide their postprocess branch and what extra parameter to bind.
    /// </summary>
    public ConcurrencyMode ConcurrencyMode { get; }

    /// <summary>
    /// The version binder, present whenever <see cref="ConcurrencyMode"/> is non-<c>Off</c> or
    /// the mapping has a version-annotated member. Operations use it from <c>Postprocess</c> to
    /// write the new version back onto the document without needing to re-walk
    /// <see cref="ReadBinders"/>.
    /// </summary>
    public IVersionMetadataBinder<TDoc>? VersionBinder { get; }

    /// <summary>
    /// The numeric revision binder, present when <see cref="ConcurrencyMode"/> is
    /// <see cref="ConcurrencyMode.Numeric"/> or the mapping has a version-annotated numeric
    /// member. Operations use it from <c>Postprocess</c> to write the new revision back onto
    /// the document.
    /// </summary>
    public IRevisionMetadataBinder<TDoc>? RevisionBinder { get; }

    /// <summary>
    /// Zero-based index of the version binder inside <see cref="ReadBinders"/>, or <c>-1</c>
    /// when version isn't in the read set. Each selector adds its own first-metadata-column
    /// offset to get the actual reader column ordinal.
    /// </summary>
    public int VersionReadIndex { get; }

    /// <summary>
    /// Store-agnostic document-type alias → .NET <see cref="Type"/> resolver, present when the
    /// mapping is hierarchical, otherwise <c>null</c>. Selectors use it for polymorphic
    /// deserialization; the owning store's builder captures its mapping's alias lookup as a
    /// delegate.
    /// </summary>
    public Func<string, Type>? ResolveDocumentType { get; }

    /// <summary>
    /// Zero-based index of the document-type binder inside <see cref="ReadBinders"/>, or
    /// <c>-1</c> when the mapping isn't hierarchical. Selectors translate it to a column
    /// ordinal the same way as <see cref="VersionReadIndex"/>.
    /// </summary>
    public int DocTypeReadIndex { get; }
}
