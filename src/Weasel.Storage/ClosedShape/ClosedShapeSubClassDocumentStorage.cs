#nullable enable
using System.Data.Common;
using JasperFx.MultiTenancy;
using Weasel.Core;
using Weasel.Core.SqlGeneration;

namespace Weasel.Storage;

/// <summary>
///     The storage for a registered sub-class in a document hierarchy: everything delegates to the
///     root type's storage, which owns the table, the descriptor and the hierarchical selectors, and
///     the read paths narrow to the sub-class (weasel#811). What differs between stores comes from the
///     <see cref="IClosedShapeSubClassRules{TDoc,TRoot,TId}" />.
/// </summary>
/// <remarks>
///     <para>
///         <b>Writes delegate untouched.</b> The root's write operations stamp the discriminator from the
///         document's runtime type, so a sub-class instance already goes in with its own alias.
///     </para>
///     <para>
///         <b>Queries narrow in SQL, loads by id narrow in memory.</b> Queries carry the rules object's
///         filters; a load goes through the root and returns default when the row is a sibling
///         sub-class. Load-many keeps only the rows of this sub-class.
///     </para>
///     <para>
///         <b>Every defaulted member of the storage interfaces is forwarded explicitly</b>, including the
///         mapped version and revision members: leaving one out compiles and quietly answers the
///         interface's default, which for <see cref="MappedVersionFor" /> cost a sub-class its
///         concurrency guard in Fisher (fisher#394).
///     </para>
///     <para>
///         Public and unsealed so a store can derive it to carry its own storage interfaces; its
///         members are not virtual, because the store-specific parts are on the rules object.
///     </para>
/// </remarks>
/// <typeparam name="TDoc">The sub-class.</typeparam>
/// <typeparam name="TRoot">The hierarchy's root document type.</typeparam>
/// <typeparam name="TId">The hierarchy's id type.</typeparam>
public class ClosedShapeSubClassDocumentStorage<TDoc, TRoot, TId>: IDocumentStorage<TDoc, TId>
    where TDoc : notnull, TRoot
    where TRoot : notnull
    where TId : notnull
{
    private readonly ISqlFragment? _defaultWhere;

    /// <summary>Wrap one of the root's storages.</summary>
    /// <param name="root">The hierarchy root's storage, in the tracking mode this storage serves.</param>
    /// <param name="rules">What differs from store to store about the sub-class's storage.</param>
    public ClosedShapeSubClassDocumentStorage(IDocumentStorage<TRoot, TId> root,
        IClosedShapeSubClassRules<TDoc, TRoot, TId> rules)
    {
        Root = root ?? throw new ArgumentNullException(nameof(root));
        Rules = rules ?? throw new ArgumentNullException(nameof(rules));

        _defaultWhere = rules.DefaultWhereFragment(root);
    }

    /// <summary>The hierarchy root's storage this wraps.</summary>
    public IDocumentStorage<TRoot, TId> Root { get; }

    /// <summary>The store-supplied sub-class rules.</summary>
    public IClosedShapeSubClassRules<TDoc, TRoot, TId> Rules { get; }

    // ---- shape: the hierarchy's ----

    /// <summary>
    ///     The <em>root</em> type: callers read it to find the table and the identity space, and both
    ///     belong to the hierarchy.
    /// </summary>
    public Type SourceType => typeof(TRoot);

    /// <inheritdoc />
    public Type SelectedType => typeof(TDoc);

    /// <inheritdoc />
    public Type DocumentType => typeof(TDoc);

    /// <inheritdoc />
    public Type IdType => typeof(TId);

    /// <inheritdoc />
    public bool UseOptimisticConcurrency => Root.UseOptimisticConcurrency;

    /// <inheritdoc />
    public bool UseNumericRevisions => Root.UseNumericRevisions;

    /// <inheritdoc />
    public IOperationFragment DeleteFragment => Root.DeleteFragment;

    /// <inheritdoc />
    public IOperationFragment HardDeleteFragment => Root.HardDeleteFragment;

    /// <inheritdoc />
    public IReadOnlyList<IDuplicatedField> DuplicatedFields => Root.DuplicatedFields;

    /// <inheritdoc />
    public DbObjectName TableName => Root.TableName;

    /// <inheritdoc />
    public TenancyStyle TenancyStyle => Root.TenancyStyle;

    /// <inheritdoc />
    public string FromObject => Root.FromObject;

    /// <inheritdoc />
    public string[] SelectFields() => Root.SelectFields();

    /// <inheritdoc />
    public void Apply(ICommandBuilder builder) => Root.Apply(builder);

    /// <summary>The root's selector, its rows cast down to <typeparamref name="TDoc" />.</summary>
    public ISelector BuildSelector(IStorageSession session)
        => new StorageCastingSelector<TDoc, TRoot>((ISelector<TRoot>)Root.BuildSelector(session));

    // ---- filters: the rules object's ----

    /// <inheritdoc cref="IClosedShapeSubClassRules{TDoc,TRoot,TId}.FilterDocuments" />
    public ISqlFragment FilterDocuments(ISqlFragment query, IStorageSession session)
        => Rules.FilterDocuments(Root, query, session);

    /// <inheritdoc cref="IClosedShapeSubClassRules{TDoc,TRoot,TId}.DefaultWhereFragment" />
    public ISqlFragment? DefaultWhereFragment() => _defaultWhere;

    /// <inheritdoc />
    public ISqlFragment ByIdFilter(TId id) => Root.ByIdFilter(id);

    /// <summary>
    ///     Run the rules object's <see cref="IClosedShapeSubClassRules{TDoc,TRoot,TId}.TruncateSql" />,
    ///     or truncate the root's whole table when it has none.
    /// </summary>
    public async Task TruncateDocumentStorageAsync(IStorageDatabase database, CancellationToken ct = default)
    {
        if (Rules.TruncateSql is not { } sql)
        {
            await Root.TruncateDocumentStorageAsync(database, ct).ConfigureAwait(false);
            return;
        }

        try
        {
            await database.RunSqlAsync(sql, ct).ConfigureAwait(false);
        }
        catch (Exception e) when (Root is IClosedShapeDocumentStorage<TRoot, TId> closed
                                  && closed.Descriptor.Dialect.IsUndefinedTable(e))
        {
            // the table doesn't exist yet: nothing to truncate
        }
    }

    // ---- identity: the root's ----

    /// <inheritdoc />
    public TId Identity(TDoc document) => Root.Identity(document);

    /// <inheritdoc />
    public object IdentityFor(TDoc document) => Root.IdentityFor(document);

    /// <inheritdoc />
    public TId AssignIdentity(TDoc document, string tenantId, IStorageDatabase database)
        => Root.AssignIdentity(document, tenantId, database);

    /// <inheritdoc />
    public void SetIdentity(TDoc document, TId identity) => Root.SetIdentity(document, identity);

    /// <inheritdoc />
    public void SetIdentityFromString(TDoc document, string identityString)
        => Root.SetIdentityFromString(document, identityString);

    /// <inheritdoc />
    public void SetIdentityFromGuid(TDoc document, Guid identityGuid) => Root.SetIdentityFromGuid(document, identityGuid);

    /// <inheritdoc />
    public object RawIdentityValue(object id) => Root.RawIdentityValue(id);

    /// <inheritdoc />
    public object RawIdentityValue(TId id) => Root.RawIdentityValue(id);

    // ---- versions: the root's, forwarded explicitly ----

    /// <inheritdoc />
    public Guid? VersionFor(TDoc document, IStorageSession session) => Root.VersionFor(document, session);

    /// <inheritdoc />
    public Guid? MappedVersionFor(TDoc document) => Root.MappedVersionFor(document);

    /// <inheritdoc />
    public long? MappedRevisionFor(TDoc document) => Root.MappedRevisionFor(document);

    /// <inheritdoc />
    public bool TryApplyMappedVersion(TDoc document, Guid version) => Root.TryApplyMappedVersion(document, version);

    /// <inheritdoc />
    public bool TryApplyMappedRevision(TDoc document, long revision)
        => Root.TryApplyMappedRevision(document, revision);

    // ---- unit of work: the root's ----

    /// <inheritdoc />
    public void Store(IStorageSession session, TDoc document) => Root.Store(session, document);

    /// <inheritdoc />
    public void Store(IStorageSession session, TDoc document, Guid? version) => Root.Store(session, document, version);

    /// <inheritdoc />
    public void Store(IStorageSession session, TDoc document, long revision)
        => Root.Store(session, document, revision);

    /// <inheritdoc />
    public void Eject(IStorageSession session, TDoc document) => Root.Eject(session, document);

    /// <inheritdoc />
    public void EjectById(IStorageSession session, object id) => Root.EjectById(session, id);

    /// <inheritdoc />
    public void RemoveDirtyTracker(IStorageSession session, object id) => Root.RemoveDirtyTracker(session, id);

    // ---- writes: the root's, untouched ----

    /// <inheritdoc />
    public IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => Root.Insert(document, session, tenantId);

    /// <inheritdoc />
    public IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => Root.Update(document, session, tenantId);

    /// <inheritdoc />
    public IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => Root.Upsert(document, session, tenantId);

    /// <inheritdoc />
    public IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => Root.Overwrite(document, session, tenantId);

    /// <inheritdoc />
    public IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => Root.OverwriteProjected(document, tenantId);

    /// <inheritdoc />
    public IStorageOperation UpsertProjected(TDoc document, string tenantId) => Root.UpsertProjected(document, tenantId);

    /// <inheritdoc />
    public IStorageOperation InsertProjected(TDoc document, string tenantId) => Root.InsertProjected(document, tenantId);

    /// <inheritdoc />
    public IStorageOperation UpdateProjected(TDoc document, string tenantId) => Root.UpdateProjected(document, tenantId);

    // ---- deletes: the root's ----

    /// <inheritdoc />
    public IDeletion DeleteForDocument(TDoc document, string tenantId) => Root.DeleteForDocument(document, tenantId);

    /// <inheritdoc />
    public IDeletion HardDeleteForDocument(TDoc document, string tenantId)
        => Root.HardDeleteForDocument(document, tenantId);

    /// <inheritdoc />
    public IDeletion DeleteForId(TId id, string tenantId) => Root.DeleteForId(id, tenantId);

    /// <inheritdoc />
    public IDeletion HardDeleteForId(TId id, string tenantId) => Root.HardDeleteForId(id, tenantId);

    // ---- loads: the root's, narrowed to the sub-class ----

    /// <inheritdoc />
    public DbCommand BuildLoadCommand(TId id, string tenantId) => Root.BuildLoadCommand(id, tenantId);

    /// <inheritdoc />
    public DbCommand BuildLoadManyCommand(TId[] ids, string tenantId) => Root.BuildLoadManyCommand(ids, tenantId);

    /// <summary>Load through the root; default when the row is another sub-class.</summary>
    public async Task<TDoc?> LoadAsync(TId id, IStorageSession session, CancellationToken token)
        => await Root.LoadAsync(id, session, token).ConfigureAwait(false) is TDoc match ? match : default;

    /// <summary>Load through the root, keeping only this sub-class's documents.</summary>
    public async Task<IReadOnlyList<TDoc>> LoadManyAsync(TId[] ids, IStorageSession session, CancellationToken token)
        => (await Root.LoadManyAsync(ids, session, token).ConfigureAwait(false)).OfType<TDoc>().ToList();

    /// <summary>The root's session-free load; default when the row is another sub-class.</summary>
    public async Task<TDoc?> LoadProjectedAsync(TId id, IStorageDatabase database, string tenantId,
        CancellationToken token)
        => await Root.LoadProjectedAsync(id, database, tenantId, token).ConfigureAwait(false) is TDoc match
            ? match
            : default;

    /// <summary>The root's session-free load-many, keeping only this sub-class's documents.</summary>
    public async Task<IReadOnlyList<TDoc>> LoadManyProjectedAsync(TId[] ids, IStorageDatabase database,
        string tenantId, CancellationToken token)
        => (await Root.LoadManyProjectedAsync(ids, database, tenantId, token).ConfigureAwait(false))
            .OfType<TDoc>().ToList();
}
