#nullable enable
using System.Data.Common;
using JasperFx.MultiTenancy;
using Weasel.Core;
using Weasel.Core.SqlGeneration;

namespace Weasel.Storage;

/// <summary>
///     A document storage built by <see cref="ClosedShapeDocumentProviderFactory" />, or derived by a
///     store from one of the public tracking-mode bases: the non-generic view, for a store that holds
///     storages by <see cref="Type" />.
/// </summary>
public interface IClosedShapeDocumentStorage: IDocumentStorage
{
    /// <summary>The tracking mode this storage was built in.</summary>
    ClosedShapeTrackingMode TrackingMode { get; }

    /// <summary>
    ///     The store-supplied rules object, typed as <c>IClosedShapeStorageRules&lt;TDoc, TId&gt;</c>.
    ///     Type-test it for the store's own storage interfaces.
    /// </summary>
    object Rules { get; }

    /// <summary>
    ///     Point a nested session's identity-map entry for this document type at the parent session's
    ///     dictionary, and its version entries too where the rules object knows how
    ///     (<see cref="IClosedShapeStorageRules{TDoc,TId}.ShareVersions" />). Does nothing outside the
    ///     identity-map modes. Only call it for tenancy-neutral documents living in the same database.
    /// </summary>
    void ShareTenantNeutralStateWith(IStorageSession parent, IStorageSession nested);

    /// <summary>
    ///     The fragment a criteria-based undelete writes, or null when the document type is not
    ///     soft-deleted (<see cref="IClosedShapeStorageRules{TDoc,TId}.BuildUndeleteFragment" />).
    /// </summary>
    /// <remarks>
    ///     Added in weasel#811. The default throws, so an implementer outside Weasel that predates the
    ///     member is not mistaken for a document type that has nothing to undelete.
    /// </remarks>
    IOperationFragment? UndeleteFragment => throw new NotSupportedException(
        $"{GetType().FullName} predates IClosedShapeDocumentStorage.UndeleteFragment.");
}

/// <summary>
///     A document storage built by <see cref="ClosedShapeDocumentProviderFactory" />, or derived by a
///     store from one of the public tracking-mode bases.
/// </summary>
public interface IClosedShapeDocumentStorage<TDoc, TId>: IClosedShapeDocumentStorage, IDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    /// <summary>Identity, SQL typing, write SQL and binders for the document type.</summary>
    DocumentStorageDescriptor<TDoc, TId> Descriptor { get; }

    /// <summary>The store-supplied rules object.</summary>
    new IClosedShapeStorageRules<TDoc, TId> Rules { get; }

    /// <summary>
    ///     Bring one soft-deleted document back, leaving its body and version as they are; null when
    ///     the document type has nothing to bring back
    ///     (<see cref="IClosedShapeStorageRules{TDoc,TId}.BuildUndeletion" />).
    /// </summary>
    /// <remarks>
    ///     Added in weasel#811. The default throws rather than answering null, for the reason given on
    ///     <see cref="IClosedShapeDocumentStorage.UndeleteFragment" />.
    /// </remarks>
    IStorageOperation? UndeleteForId(TId id, string tenantId) => throw new NotSupportedException(
        $"{GetType().FullName} predates IClosedShapeDocumentStorage<TDoc, TId>.UndeleteForId.");

    /// <summary>
    ///     Restore one soft-deleted document live, with a new version; null when the document type has
    ///     nothing to bring back (<see cref="IClosedShapeStorageRules{TDoc,TId}.BuildRestoration" />).
    /// </summary>
    /// <remarks>
    ///     Added in weasel#811. The default throws, for the reason given on
    ///     <see cref="IClosedShapeDocumentStorage.UndeleteFragment" />.
    /// </remarks>
    IStorageOperation? RestoreForId(TId id, string tenantId) => throw new NotSupportedException(
        $"{GetType().FullName} predates IClosedShapeDocumentStorage<TDoc, TId>.RestoreForId.");
}

/// <summary>
///     The dialect-neutral base of the closed-shape storage family. Identity and SQL typing come from
///     the descriptor, command materialization from its <see cref="IStorageDialect" />, and everything
///     that differs between stores from the <see cref="IClosedShapeStorageRules{TDoc,TId}" />.
/// </summary>
/// <remarks>
///     <para>
///         <b>Derive from a tracking-mode base, not from this.</b> A store that needs its own interfaces
///         on the storage object (so that call sites type-testing a storage keep working) derives from
///         <see cref="QueryOnlyClosedShapeDocumentStorage{TDoc,TId}" />,
///         <see cref="LightweightClosedShapeDocumentStorage{TDoc,TId}" />,
///         <see cref="IdentityMapClosedShapeDocumentStorage{TDoc,TId}" /> or
///         <see cref="DirtyTrackingClosedShapeDocumentStorage{TDoc,TId}" />. Each needs only a
///         constructor: the write operations and the selector for the descriptor's
///         <see cref="ConcurrencyMode" /> are built here by default (weasel#811). Store-specific
///         behaviour belongs on the rules object, which this class consults for every such decision.
///     </para>
///     <para>
///         The storages <see cref="ClosedShapeDocumentProviderFactory" /> builds are sealed leaves fixed
///         to one concurrency mode, so they never branch on it per call. A store-derived storage
///         branches once per write on a read-only field, and builds exactly the same operations over
///         exactly the same descriptor.
///     </para>
///     <para>
///         The constructor calls <see cref="IClosedShapeStorageRules{TDoc,TId}.BuildDeleteFragment" />,
///         <see cref="IClosedShapeStorageRules{TDoc,TId}.BuildHardDeleteFragment" /> and
///         <see cref="IClosedShapeStorageRules{TDoc,TId}.BuildUndeleteFragment" /> with this storage
///         before a derived constructor has run, so those members must not read a derived class's
///         state off the storage they are handed.
///     </para>
/// </remarks>
/// <typeparam name="TDoc">The document type.</typeparam>
/// <typeparam name="TId">The document's id type.</typeparam>
public abstract class ClosedShapeDocumentStorage<TDoc, TId>: IClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    private protected readonly DocumentStorageDescriptor<TDoc, TId> _descriptor;
    private protected readonly IClosedShapeStorageRules<TDoc, TId> _rules;
    private readonly ClosedShapeReadSql _sql;

    /// <summary>Build the storage for one tracking mode.</summary>
    /// <param name="mode">The tracking mode, which picks the read SQL from the rules object.</param>
    /// <param name="descriptor">Identity, SQL typing, write SQL and binders for the document type.</param>
    /// <param name="rules">Everything about the document type's storage that differs from store to store.</param>
    protected ClosedShapeDocumentStorage(ClosedShapeTrackingMode mode, DocumentStorageDescriptor<TDoc, TId> descriptor,
        IClosedShapeStorageRules<TDoc, TId> rules)
    {
        TrackingMode = mode;
        _descriptor = descriptor ?? throw new ArgumentNullException(nameof(descriptor));
        _rules = rules ?? throw new ArgumentNullException(nameof(rules));
        _sql = rules.ReadSqlFor(mode);

        TableName = rules.TableName;
        TenancyStyle = rules.TenancyStyle;
        DuplicatedFields = rules.DuplicatedFields;

        DeleteFragment = rules.BuildDeleteFragment(this);
        HardDeleteFragment = rules.BuildHardDeleteFragment(this);
        UndeleteFragment = rules.BuildUndeleteFragment(this);
    }

    /// <inheritdoc />
    public ClosedShapeTrackingMode TrackingMode { get; }

    /// <inheritdoc />
    public DocumentStorageDescriptor<TDoc, TId> Descriptor => _descriptor;

    /// <inheritdoc />
    public IClosedShapeStorageRules<TDoc, TId> Rules => _rules;

    object IClosedShapeDocumentStorage.Rules => _rules;

    /// <summary>The descriptor's dialect: command materialization, id filters, error interpretation.</summary>
    protected IStorageDialect Dialect => _descriptor.Dialect;

    /// <inheritdoc />
    public virtual void ShareTenantNeutralStateWith(IStorageSession parent, IStorageSession nested)
    {
    }

    // ---- identity ----

    /// <summary>The document's id, read through the descriptor's identification.</summary>
    public TId Identity(TDoc document) => _descriptor.Identification.Identity(document);

    object IDocumentStorage<TDoc>.IdentityFor(TDoc document) => Identity(document);

    /// <summary>The document's id, assigning one first if it has none.</summary>
    public TId AssignIdentity(TDoc document, string tenantId, IStorageDatabase database)
        => _descriptor.Identification.AssignIfMissing(document, database);

    /// <summary>The id as the value bound to SQL (a strong-typed id's inner value, say).</summary>
    public object RawIdentityValue(TId id) => _descriptor.Identification.ToRawSqlValue(id);

    object IDocumentStorage.RawIdentityValue(object id) => RawIdentityValue((TId)id);

    /// <summary>Write the id onto the document, through the rules object.</summary>
    public void SetIdentity(TDoc document, TId identity) => _rules.SetIdentity(document, identity);

    /// <summary>Write an id given as a string onto the document, through the rules object.</summary>
    public void SetIdentityFromString(TDoc document, string identityString)
        => _rules.SetIdentityFromString(document, identityString);

    /// <summary>Write an id given as a Guid onto the document, through the rules object.</summary>
    public void SetIdentityFromGuid(TDoc document, Guid identityGuid)
        => _rules.SetIdentityFromGuid(document, identityGuid);

    // ---- shape ----

    /// <inheritdoc />
    public Type SourceType => typeof(TDoc);

    /// <inheritdoc />
    public Type SelectedType => typeof(TDoc);

    /// <inheritdoc />
    public Type DocumentType => typeof(TDoc);

    /// <inheritdoc />
    public Type IdType => typeof(TId);

    /// <inheritdoc />
    public DbObjectName TableName { get; }

    /// <inheritdoc />
    public TenancyStyle TenancyStyle { get; }

    /// <inheritdoc />
    public IReadOnlyList<IDuplicatedField> DuplicatedFields { get; }

    /// <inheritdoc />
    public bool UseOptimisticConcurrency => _descriptor.ConcurrencyMode == ConcurrencyMode.Optimistic;

    /// <inheritdoc />
    public bool UseNumericRevisions => _descriptor.ConcurrencyMode == ConcurrencyMode.Numeric;

    /// <inheritdoc />
    public string FromObject => TableName.QualifiedName;

    /// <inheritdoc />
    public string[] SelectFields() => _sql.SelectFields;

    /// <inheritdoc />
    public void Apply(ICommandBuilder builder) => builder.Append(_sql.SelectClause);

    /// <summary>
    ///     The selector this tracking mode reads rows with. Each tracking-mode base supplies the one for
    ///     the descriptor's <see cref="ConcurrencyMode" />; the column order it reads is the
    ///     <see cref="ClosedShapeReadSql" /> contract.
    /// </summary>
    public abstract ISelector BuildSelector(IStorageSession session);

    // ---- filters ----

    /// <inheritdoc />
    public ISqlFragment FilterDocuments(ISqlFragment query, IStorageSession session)
        => _rules.FilterDocuments(query, session);

    /// <inheritdoc />
    public ISqlFragment? DefaultWhereFragment() => _rules.DefaultWhereFragment();

    /// <summary>The dialect's filter for one id.</summary>
    public ISqlFragment ByIdFilter(TId id) => Dialect.ByIdFilter(RawIdentityValue(id));

    // ---- versions ----

    /// <summary>The version this session holds for the document, if it holds one.</summary>
    public Guid? VersionFor(TDoc document, IStorageSession session)
    {
        ArgumentNullException.ThrowIfNull(document);
        return session.Versions.VersionFor<TDoc, TId>(Identity(document));
    }

    /// <inheritdoc />
    public Guid? MappedVersionFor(TDoc document) => _rules.MappedVersionFor(document);

    /// <inheritdoc />
    public long? MappedRevisionFor(TDoc document) => _rules.MappedRevisionFor(document);

    /// <inheritdoc />
    public bool TryApplyMappedVersion(TDoc document, Guid version) => _rules.TryApplyMappedVersion(document, version);

    /// <inheritdoc />
    public bool TryApplyMappedRevision(TDoc document, long revision)
        => _rules.TryApplyMappedRevision(document, revision);

    private protected void refuseNumericRevisionsIfUnsupported()
    {
        if (_rules.NumericRevisionsNotSupportedReason is { } reason)
        {
            throw new NotSupportedException(reason);
        }
    }

    // ---- unit of work ----

    /// <summary>Register the document for storage in the session's unit of work.</summary>
    public abstract void Store(IStorageSession session, TDoc document);

    /// <summary>
    ///     Register the document for storage with the Guid version the write is expected to find; a null
    ///     version clears any the session holds.
    /// </summary>
    public abstract void Store(IStorageSession session, TDoc document, Guid? version);

    /// <summary>
    ///     Register the document for storage with the numeric revision the write is expected to find.
    /// </summary>
    public abstract void Store(IStorageSession session, TDoc document, long revision);

    /// <summary>Drop the document from whatever the session tracks it in.</summary>
    public abstract void Eject(IStorageSession session, TDoc document);

    /// <inheritdoc />
    public void EjectById(IStorageSession session, object id)
    {
        if (session.ItemMap.TryGetValue(typeof(TDoc), out var items) && items is Dictionary<TId, TDoc> map)
        {
            map.Remove((TId)id);
        }
    }

    /// <inheritdoc />
    public void RemoveDirtyTracker(IStorageSession session, object id)
    {
        var trackers = session.ChangeTrackers;
        for (var i = trackers.Count - 1; i >= 0; i--)
        {
            if (trackers[i].Document is TDoc doc && Identity(doc).Equals(id))
            {
                trackers.RemoveAt(i);
            }
        }
    }

    // The default write factories (weasel#811). The factory's sealed leaves override all eight with
    // one concurrency mode each; a store-derived storage reaches these and branches on the
    // descriptor's mode, building the same operation over the same descriptor the leaf would.

    /// <summary>
    ///     The insert for the descriptor's <see cref="ConcurrencyMode" />, over the descriptor the rules
    ///     object's <see cref="IClosedShapeStorageRules{TDoc,TId}.WriteDescriptorFor" /> chooses.
    /// </summary>
    public virtual IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => _descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Off => UnversionedClosedShapeWrites<TDoc, TId>.Insert(this, document, session, tenantId),
            ConcurrencyMode.Optimistic => OptimisticClosedShapeWrites<TDoc, TId>.Insert(this, document, session, tenantId),
            ConcurrencyMode.Numeric => NumericClosedShapeWrites<TDoc, TId>.Insert(this, document, session, tenantId),
            _ => throw unknownConcurrencyMode()
        };

    /// <summary>The update for the descriptor's <see cref="ConcurrencyMode" />. See <see cref="Insert" />.</summary>
    public virtual IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => _descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Off => UnversionedClosedShapeWrites<TDoc, TId>.Update(this, document, session, tenantId),
            ConcurrencyMode.Optimistic => OptimisticClosedShapeWrites<TDoc, TId>.Update(this, document, session, tenantId),
            ConcurrencyMode.Numeric => NumericClosedShapeWrites<TDoc, TId>.Update(this, document, session, tenantId),
            _ => throw unknownConcurrencyMode()
        };

    /// <summary>The upsert for the descriptor's <see cref="ConcurrencyMode" />. See <see cref="Insert" />.</summary>
    public virtual IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => _descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Off => UnversionedClosedShapeWrites<TDoc, TId>.Upsert(this, document, session, tenantId),
            ConcurrencyMode.Optimistic => OptimisticClosedShapeWrites<TDoc, TId>.Upsert(this, document, session, tenantId),
            ConcurrencyMode.Numeric => NumericClosedShapeWrites<TDoc, TId>.Upsert(this, document, session, tenantId),
            _ => throw unknownConcurrencyMode()
        };

    /// <summary>The overwrite for the descriptor's <see cref="ConcurrencyMode" />. See <see cref="Insert" />.</summary>
    public virtual IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => _descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Off => UnversionedClosedShapeWrites<TDoc, TId>.Overwrite(this, document, session, tenantId),
            ConcurrencyMode.Optimistic => OptimisticClosedShapeWrites<TDoc, TId>.Overwrite(this, document, session, tenantId),
            ConcurrencyMode.Numeric => NumericClosedShapeWrites<TDoc, TId>.Overwrite(this, document, session, tenantId),
            _ => throw unknownConcurrencyMode()
        };

    /// <summary>
    ///     The session-free overwrite for the descriptor's <see cref="ConcurrencyMode" />: no version
    ///     tracker is read, so parallel projection workers sharing a session never touch it.
    /// </summary>
    public virtual IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => _descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Off => UnversionedClosedShapeWrites<TDoc, TId>.OverwriteProjected(this, document, tenantId),
            ConcurrencyMode.Optimistic => OptimisticClosedShapeWrites<TDoc, TId>.OverwriteProjected(this, document, tenantId),
            ConcurrencyMode.Numeric => NumericClosedShapeWrites<TDoc, TId>.OverwriteProjected(this, document, tenantId),
            _ => throw unknownConcurrencyMode()
        };

    /// <summary>The session-free upsert. See <see cref="OverwriteProjected" />.</summary>
    public virtual IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => _descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Off => UnversionedClosedShapeWrites<TDoc, TId>.UpsertProjected(this, document, tenantId),
            ConcurrencyMode.Optimistic => OptimisticClosedShapeWrites<TDoc, TId>.UpsertProjected(this, document, tenantId),
            ConcurrencyMode.Numeric => NumericClosedShapeWrites<TDoc, TId>.UpsertProjected(this, document, tenantId),
            _ => throw unknownConcurrencyMode()
        };

    /// <summary>The session-free insert. See <see cref="OverwriteProjected" />.</summary>
    public virtual IStorageOperation InsertProjected(TDoc document, string tenantId)
        => _descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Off => UnversionedClosedShapeWrites<TDoc, TId>.InsertProjected(this, document, tenantId),
            ConcurrencyMode.Optimistic => OptimisticClosedShapeWrites<TDoc, TId>.InsertProjected(this, document, tenantId),
            ConcurrencyMode.Numeric => NumericClosedShapeWrites<TDoc, TId>.InsertProjected(this, document, tenantId),
            _ => throw unknownConcurrencyMode()
        };

    /// <summary>The session-free update. See <see cref="OverwriteProjected" />.</summary>
    public virtual IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => _descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Off => UnversionedClosedShapeWrites<TDoc, TId>.UpdateProjected(this, document, tenantId),
            ConcurrencyMode.Optimistic => OptimisticClosedShapeWrites<TDoc, TId>.UpdateProjected(this, document, tenantId),
            ConcurrencyMode.Numeric => NumericClosedShapeWrites<TDoc, TId>.UpdateProjected(this, document, tenantId),
            _ => throw unknownConcurrencyMode()
        };

    private protected ArgumentOutOfRangeException unknownConcurrencyMode()
        => new(nameof(ConcurrencyMode),
            $"No closed-shape storage for tracking mode {TrackingMode} with concurrency mode {_descriptor.ConcurrencyMode}.");

    // ---- deletes ----

    /// <inheritdoc />
    public IOperationFragment DeleteFragment { get; }

    /// <inheritdoc />
    public IOperationFragment HardDeleteFragment { get; }

    /// <inheritdoc />
    public IOperationFragment? UndeleteFragment { get; }

    /// <summary>The delete (soft where the document type is soft-deleted) for one id.</summary>
    public IDeletion DeleteForId(TId id, string tenantId)
    {
        var deletion = _rules.BuildDeletion(this, DeleteFragment, id, ByIdFilter(id), tenantId);
        deletion.Id = id;
        return deletion;
    }

    /// <summary>The hard delete for one id.</summary>
    public IDeletion HardDeleteForId(TId id, string tenantId)
    {
        var deletion = _rules.BuildDeletion(this, HardDeleteFragment, id, ByIdFilter(id), tenantId);
        deletion.Id = id;
        return deletion;
    }

    /// <inheritdoc />
    public IDeletion DeleteForDocument(TDoc document, string tenantId)
    {
        var deletion = DeleteForId(Identity(document), tenantId);
        deletion.Document = document;
        return deletion;
    }

    /// <inheritdoc />
    public IDeletion HardDeleteForDocument(TDoc document, string tenantId)
    {
        var deletion = HardDeleteForId(Identity(document), tenantId);
        deletion.Document = document;
        return deletion;
    }

    /// <inheritdoc />
    public IStorageOperation? UndeleteForId(TId id, string tenantId)
        => _rules.BuildUndeletion(this, UndeleteFragment, id, ByIdFilter(id), tenantId);

    /// <inheritdoc />
    public IStorageOperation? RestoreForId(TId id, string tenantId)
        => _rules.BuildRestoration(this, id, ByIdFilter(id), tenantId);

    /// <summary>
    ///     Run the rules object's <see cref="IClosedShapeStorageRules{TDoc,TId}.TruncateSql" />; a table
    ///     that does not exist yet has nothing to truncate.
    /// </summary>
    public async Task TruncateDocumentStorageAsync(IStorageDatabase database, CancellationToken ct = default)
    {
        try
        {
            await database.RunSqlAsync(_rules.TruncateSql, ct).ConfigureAwait(false);
        }
        catch (Exception e) when (Dialect.IsUndefinedTable(e))
        {
            // the table doesn't exist yet: nothing to truncate
        }
    }

    // ---- loads ----

    /// <summary>The single-document load command for this tracking mode's read SQL.</summary>
    public DbCommand BuildLoadCommand(TId id, string tenantId)
        => Dialect.BuildLoadCommand(_sql.LoaderSql, RawIdentityValue(id),
            TenancyStyle == TenancyStyle.Conjoined ? tenantId : null);

    /// <summary>The load-many command for this tracking mode's read SQL.</summary>
    public DbCommand BuildLoadManyCommand(TId[] ids, string tenantId)
        => Dialect.BuildLoadManyCommand(_sql.LoadArraySql, BuildManyIdParameter(ids),
            TenancyStyle == TenancyStyle.Conjoined ? tenantId : null);

    /// <summary>The id-array parameter for a load-many, with each id converted to its raw SQL value.</summary>
    public DbParameter BuildManyIdParameter(TId[] ids)
        => Dialect.CreateIdArrayParameter(
            Array.ConvertAll(ids, id => _descriptor.Identification.ToRawSqlValue(id)),
            _descriptor.Identification.RawSqlType);

    /// <summary>Load one document through this tracking mode.</summary>
    public abstract Task<TDoc?> LoadAsync(TId id, IStorageSession session, CancellationToken token);

    /// <summary>Load several documents through this tracking mode.</summary>
    public abstract Task<IReadOnlyList<TDoc>> LoadManyAsync(TId[] ids, IStorageSession session, CancellationToken token);

    /// <summary>
    ///     The session-free load for projection storage, on a connection of its own. Overridable so a
    ///     store can add a guard (an empty-id short circuit, say) without re-deriving the read.
    /// </summary>
    public virtual Task<TDoc?> LoadProjectedAsync(TId id, IStorageDatabase database, string tenantId,
        CancellationToken token)
        => ClosedShapeProjectionLoader<TDoc, TId>.LoadAsync(
            BuildLoadCommand(id, tenantId), _descriptor, _descriptor.Serializer, database, token);

    /// <summary>The session-free load-many for projection storage. See <see cref="LoadProjectedAsync" />.</summary>
    public virtual Task<IReadOnlyList<TDoc>> LoadManyProjectedAsync(TId[] ids, IStorageDatabase database,
        string tenantId, CancellationToken token)
        => ClosedShapeProjectionLoader<TDoc, TId>.LoadManyAsync(
            BuildLoadManyCommand(ids, tenantId), _descriptor, _descriptor.Serializer, database, token);

    /// <summary>Read one document through this mode's selector.</summary>
    protected async Task<TDoc?> queryOneAsync(TId id, IStorageSession session, CancellationToken token)
    {
        var command = BuildLoadCommand(id, session.TenantId);
        var selector = (ISelector<TDoc>)BuildSelector(session);

        await using var reader = await session.ExecuteReaderAsync(command, token).ConfigureAwait(false);
        try
        {
            if (!await reader.ReadAsync(token).ConfigureAwait(false))
            {
                return default;
            }

            return await selector.ResolveAsync(reader, token).ConfigureAwait(false);
        }
        finally
        {
            await reader.CloseAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Read the documents for <paramref name="ids" /> through this mode's selector, appending to <paramref name="list" />.</summary>
    protected async Task queryManyAsync(TId[] ids, IStorageSession session, List<TDoc> list, CancellationToken token)
    {
        var command = BuildLoadManyCommand(ids, session.TenantId);
        var selector = (ISelector<TDoc>)BuildSelector(session);

        await using var reader = await session.ExecuteReaderAsync(command, token).ConfigureAwait(false);
        try
        {
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                list.Add(await selector.ResolveAsync(reader, token).ConfigureAwait(false));
            }
        }
        finally
        {
            await reader.CloseAsync().ConfigureAwait(false);
        }
    }

    // ---- write-descriptor resolution ----

    internal DocumentStorageDescriptor<TDoc, TId> WriteDescriptor(OperationRole role, TDoc document,
        IStorageSession? session)
        => _rules.WriteDescriptorFor(role, document, session, _descriptor);
}
