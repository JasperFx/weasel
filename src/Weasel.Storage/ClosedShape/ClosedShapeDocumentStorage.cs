#nullable enable
using System.Data.Common;
using JasperFx.MultiTenancy;
using Weasel.Core;
using Weasel.Core.SqlGeneration;

namespace Weasel.Storage;

/// <summary>
///     A document storage built by <see cref="ClosedShapeDocumentProviderFactory" />: the non-generic
///     view, for a store that holds storages by <see cref="Type" />.
/// </summary>
public interface IClosedShapeDocumentStorage: IDocumentStorage
{
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
}

/// <summary>
///     A document storage built by <see cref="ClosedShapeDocumentProviderFactory" />.
/// </summary>
public interface IClosedShapeDocumentStorage<TDoc, TId>: IClosedShapeDocumentStorage, IDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    DocumentStorageDescriptor<TDoc, TId> Descriptor { get; }

    new IClosedShapeStorageRules<TDoc, TId> Rules { get; }
}

/// <summary>
///     The dialect-neutral base of the closed-shape storage family. Identity and SQL typing come from
///     the descriptor, command materialization from its <see cref="IStorageDialect" />, and everything
///     that differs between stores from the <see cref="IClosedShapeStorageRules{TDoc,TId}" />.
/// </summary>
internal abstract class ClosedShapeDocumentStorage<TDoc, TId>: IClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    protected readonly DocumentStorageDescriptor<TDoc, TId> _descriptor;
    protected readonly IClosedShapeStorageRules<TDoc, TId> _rules;
    private readonly ClosedShapeReadSql _sql;

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
    }

    public ClosedShapeTrackingMode TrackingMode { get; }

    public DocumentStorageDescriptor<TDoc, TId> Descriptor => _descriptor;

    public IClosedShapeStorageRules<TDoc, TId> Rules => _rules;

    object IClosedShapeDocumentStorage.Rules => _rules;

    protected IStorageDialect Dialect => _descriptor.Dialect;

    public virtual void ShareTenantNeutralStateWith(IStorageSession parent, IStorageSession nested)
    {
    }

    // ---- identity ----

    public TId Identity(TDoc document) => _descriptor.Identification.Identity(document);

    object IDocumentStorage<TDoc>.IdentityFor(TDoc document) => Identity(document);

    public TId AssignIdentity(TDoc document, string tenantId, IStorageDatabase database)
        => _descriptor.Identification.AssignIfMissing(document, database);

    public object RawIdentityValue(TId id) => _descriptor.Identification.ToRawSqlValue(id);

    object IDocumentStorage.RawIdentityValue(object id) => RawIdentityValue((TId)id);

    public void SetIdentity(TDoc document, TId identity) => _rules.SetIdentity(document, identity);

    public void SetIdentityFromString(TDoc document, string identityString)
        => _rules.SetIdentityFromString(document, identityString);

    public void SetIdentityFromGuid(TDoc document, Guid identityGuid)
        => _rules.SetIdentityFromGuid(document, identityGuid);

    // ---- shape ----

    public Type SourceType => typeof(TDoc);
    public Type SelectedType => typeof(TDoc);
    public Type DocumentType => typeof(TDoc);
    public Type IdType => typeof(TId);

    public DbObjectName TableName { get; }
    public TenancyStyle TenancyStyle { get; }
    public IReadOnlyList<IDuplicatedField> DuplicatedFields { get; }

    public bool UseOptimisticConcurrency => _descriptor.ConcurrencyMode == ConcurrencyMode.Optimistic;
    public bool UseNumericRevisions => _descriptor.ConcurrencyMode == ConcurrencyMode.Numeric;

    public string FromObject => TableName.QualifiedName;

    public string[] SelectFields() => _sql.SelectFields;

    public void Apply(ICommandBuilder builder) => builder.Append(_sql.SelectClause);

    public abstract ISelector BuildSelector(IStorageSession session);

    // ---- filters ----

    public ISqlFragment FilterDocuments(ISqlFragment query, IStorageSession session)
        => _rules.FilterDocuments(query, session);

    public ISqlFragment? DefaultWhereFragment() => _rules.DefaultWhereFragment();

    public ISqlFragment ByIdFilter(TId id) => Dialect.ByIdFilter(RawIdentityValue(id));

    // ---- versions ----

    public Guid? VersionFor(TDoc document, IStorageSession session)
    {
        ArgumentNullException.ThrowIfNull(document);
        return session.Versions.VersionFor<TDoc, TId>(Identity(document));
    }

    public Guid? MappedVersionFor(TDoc document) => _rules.MappedVersionFor(document);

    public long? MappedRevisionFor(TDoc document) => _rules.MappedRevisionFor(document);

    protected void refuseNumericRevisionsIfUnsupported()
    {
        if (_rules.NumericRevisionsNotSupportedReason is { } reason)
        {
            throw new NotSupportedException(reason);
        }
    }

    // ---- unit of work ----

    public abstract void Store(IStorageSession session, TDoc document);
    public abstract void Store(IStorageSession session, TDoc document, Guid? version);
    public abstract void Store(IStorageSession session, TDoc document, long revision);
    public abstract void Eject(IStorageSession session, TDoc document);

    public void EjectById(IStorageSession session, object id)
    {
        if (session.ItemMap.TryGetValue(typeof(TDoc), out var items) && items is Dictionary<TId, TDoc> map)
        {
            map.Remove((TId)id);
        }
    }

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

    public abstract IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId);
    public abstract IStorageOperation Update(TDoc document, IStorageSession session, string tenantId);
    public abstract IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId);
    public abstract IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId);
    public abstract IStorageOperation OverwriteProjected(TDoc document, string tenantId);
    public abstract IStorageOperation UpsertProjected(TDoc document, string tenantId);
    public abstract IStorageOperation InsertProjected(TDoc document, string tenantId);
    public abstract IStorageOperation UpdateProjected(TDoc document, string tenantId);

    // ---- deletes ----

    public IOperationFragment DeleteFragment { get; }
    public IOperationFragment HardDeleteFragment { get; }

    public IDeletion DeleteForId(TId id, string tenantId)
    {
        var deletion = _rules.BuildDeletion(this, DeleteFragment, id, ByIdFilter(id), tenantId);
        deletion.Id = id;
        return deletion;
    }

    public IDeletion HardDeleteForId(TId id, string tenantId)
    {
        var deletion = _rules.BuildDeletion(this, HardDeleteFragment, id, ByIdFilter(id), tenantId);
        deletion.Id = id;
        return deletion;
    }

    public IDeletion DeleteForDocument(TDoc document, string tenantId)
    {
        var deletion = DeleteForId(Identity(document), tenantId);
        deletion.Document = document;
        return deletion;
    }

    public IDeletion HardDeleteForDocument(TDoc document, string tenantId)
    {
        var deletion = HardDeleteForId(Identity(document), tenantId);
        deletion.Document = document;
        return deletion;
    }

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

    public DbCommand BuildLoadCommand(TId id, string tenantId)
        => Dialect.BuildLoadCommand(_sql.LoaderSql, RawIdentityValue(id),
            TenancyStyle == TenancyStyle.Conjoined ? tenantId : null);

    public DbCommand BuildLoadManyCommand(TId[] ids, string tenantId)
        => Dialect.BuildLoadManyCommand(_sql.LoadArraySql, BuildManyIdParameter(ids),
            TenancyStyle == TenancyStyle.Conjoined ? tenantId : null);

    public DbParameter BuildManyIdParameter(TId[] ids)
        => Dialect.CreateIdArrayParameter(
            Array.ConvertAll(ids, id => _descriptor.Identification.ToRawSqlValue(id)),
            _descriptor.Identification.RawSqlType);

    public abstract Task<TDoc?> LoadAsync(TId id, IStorageSession session, CancellationToken token);

    public abstract Task<IReadOnlyList<TDoc>> LoadManyAsync(TId[] ids, IStorageSession session, CancellationToken token);

    public virtual Task<TDoc?> LoadProjectedAsync(TId id, IStorageDatabase database, string tenantId,
        CancellationToken token)
        => ClosedShapeProjectionLoader<TDoc, TId>.LoadAsync(
            BuildLoadCommand(id, tenantId), _descriptor, _descriptor.Serializer, database, token);

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
