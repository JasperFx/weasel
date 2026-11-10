#nullable enable
namespace Weasel.Storage;

// The four tracking-mode bases of the closed-shape family. The identity-map semantics are Marten's,
// as decided for weasel#740: a second instance under a mapped id is refused (an IEquatable<T>
// document is taken at its word), an identity map keyed by another id type is refused, a revision
// is recorded on Store and a revision of zero clears it, and a nested session can share the
// parent's identity map.

/// <summary>Read-only storage: writes are refused, Store/Eject do nothing.</summary>
internal sealed class QueryOnlyClosedShapeDocumentStorage<TDoc, TId>: ClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public QueryOnlyClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor,
        IClosedShapeStorageRules<TDoc, TId> rules)
        : base(ClosedShapeTrackingMode.QueryOnly, descriptor, rules)
    {
    }

    public override void Store(IStorageSession session, TDoc document)
    {
    }

    public override void Store(IStorageSession session, TDoc document, Guid? version)
    {
    }

    public override void Store(IStorageSession session, TDoc document, long revision)
    {
    }

    public override void Eject(IStorageSession session, TDoc document)
    {
    }

    public override Task<TDoc?> LoadAsync(TId id, IStorageSession session, CancellationToken token)
        => queryOneAsync(id, session, token);

    public override async Task<IReadOnlyList<TDoc>> LoadManyAsync(TId[] ids, IStorageSession session,
        CancellationToken token)
    {
        var list = new List<TDoc>();
        await queryManyAsync(ids, session, list, token).ConfigureAwait(false);
        return list;
    }

    // The query-only read layout has no id column, so the session-free projection loader, which
    // reads the writeable layout, cannot run over it. Projection storage never holds a query-only
    // storage.
    public override Task<TDoc?> LoadProjectedAsync(TId id, IStorageDatabase database, string tenantId,
        CancellationToken token)
        => throw refused(nameof(LoadProjectedAsync));

    public override Task<IReadOnlyList<TDoc>> LoadManyProjectedAsync(TId[] ids, IStorageDatabase database,
        string tenantId, CancellationToken token)
        => throw refused(nameof(LoadManyProjectedAsync));

    public override IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => throw refused(nameof(Insert));

    public override IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => throw refused(nameof(Update));

    public override IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => throw refused(nameof(Upsert));

    public override IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => throw refused(nameof(Overwrite));

    public override IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => throw refused(nameof(OverwriteProjected));

    public override IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => throw refused(nameof(UpsertProjected));

    public override IStorageOperation InsertProjected(TDoc document, string tenantId)
        => throw refused(nameof(InsertProjected));

    public override IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => throw refused(nameof(UpdateProjected));

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalClosedShapeQueryOnlySelector<TDoc, TId>(session, _descriptor)
            : new FlatClosedShapeQueryOnlySelector<TDoc, TId>(session, _descriptor);

    private static NotSupportedException refused(string member)
        => new($"Query-only storage for {typeof(TDoc).Name} does not support {member}.");
}

/// <summary>
///     Lightweight storage: no identity map. Concurrency-mode leaves supply the write operations and
///     the selector.
/// </summary>
internal abstract class LightweightClosedShapeDocumentStorage<TDoc, TId>: ClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    protected LightweightClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor,
        IClosedShapeStorageRules<TDoc, TId> rules)
        : base(ClosedShapeTrackingMode.Lightweight, descriptor, rules)
    {
    }

    public sealed override void Store(IStorageSession session, TDoc document)
    {
        var id = AssignIdentity(document, session.TenantId, session.Database);
        session.MarkAsAddedForStorage(id, document);
    }

    public sealed override void Store(IStorageSession session, TDoc document, Guid? version)
    {
        var id = AssignIdentity(document, session.TenantId, session.Database);
        session.MarkAsAddedForStorage(id, document);

        if (version.HasValue)
        {
            session.Versions.StoreVersion<TDoc, TId>(id, version.Value);
        }
        else
        {
            session.Versions.ClearVersion<TDoc, TId>(id);
        }
    }

    public sealed override void Store(IStorageSession session, TDoc document, long revision)
    {
        refuseNumericRevisionsIfUnsupported();

        var id = AssignIdentity(document, session.TenantId, session.Database);
        session.MarkAsAddedForStorage(id, document);

        if (revision > 0)
        {
            session.Versions.StoreRevision<TDoc, TId>(id, revision);
        }
        else
        {
            session.Versions.ClearRevision<TDoc, TId>(id);
        }
    }

    public sealed override void Eject(IStorageSession session, TDoc document)
    {
        // No identity map to eject from.
    }

    public sealed override Task<TDoc?> LoadAsync(TId id, IStorageSession session, CancellationToken token)
        => queryOneAsync(id, session, token);

    public sealed override async Task<IReadOnlyList<TDoc>> LoadManyAsync(TId[] ids, IStorageSession session,
        CancellationToken token)
    {
        var list = new List<TDoc>();
        await queryManyAsync(ids, session, list, token).ConfigureAwait(false);
        return list;
    }

    /// <summary>
    ///     For a session shared by parallel projection workers, the selector that captures no
    ///     versions: the version-capturing selectors write to session-shared dictionaries per row,
    ///     which races across workers. The row layout is the same, so mapped version members are
    ///     still set on the document. Null for any other session.
    /// </summary>
    protected ISelector? sessionFreeSelector(IStorageSession session)
    {
        if (!_rules.IsSharedProjectionSession(session))
        {
            return null;
        }

        return _descriptor.ResolveDocumentType is not null
            ? new HierarchicalUnversionedClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor)
            : new FlatUnversionedClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor);
    }
}

/// <summary>
///     Identity-map storage: loaded and stored documents are kept in the session's identity map.
/// </summary>
internal abstract class IdentityMapClosedShapeDocumentStorage<TDoc, TId>: ClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    protected IdentityMapClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor,
        IClosedShapeStorageRules<TDoc, TId> rules)
        : this(ClosedShapeTrackingMode.IdentityMap, descriptor, rules)
    {
    }

    protected IdentityMapClosedShapeDocumentStorage(ClosedShapeTrackingMode mode,
        DocumentStorageDescriptor<TDoc, TId> descriptor, IClosedShapeStorageRules<TDoc, TId> rules)
        : base(mode, descriptor, rules)
    {
    }

    public sealed override void ShareTenantNeutralStateWith(IStorageSession parent, IStorageSession nested)
    {
        if (ReferenceEquals(parent.ItemMap, nested.ItemMap))
        {
            return;
        }

        if (parent.ItemMap.TryGetValue(typeof(TDoc), out var items))
        {
            // A mismatched key type is diagnosed (and refused) by the load and store paths. Don't
            // propagate a broken entry into the nested session here.
            if (items is not Dictionary<TId, TDoc>)
            {
                return;
            }
        }
        else
        {
            items = new Dictionary<TId, TDoc>();
            parent.ItemMap[typeof(TDoc)] = items;
        }

        nested.ItemMap[typeof(TDoc)] = items;

        if (UseOptimisticConcurrency || UseNumericRevisions)
        {
            _rules.ShareVersions(parent, nested);
        }
    }

    public sealed override void Eject(IStorageSession session, TDoc document)
    {
        if (session.ItemMap.TryGetValue(typeof(TDoc), out var items) && items is Dictionary<TId, TDoc> map)
        {
            map.Remove(Identity(document));
        }
    }

    public sealed override void Store(IStorageSession session, TDoc document)
    {
        store(session, document);
    }

    public sealed override void Store(IStorageSession session, TDoc document, Guid? version)
    {
        var id = store(session, document);

        if (version.HasValue)
        {
            session.Versions.StoreVersion<TDoc, TId>(id, version.Value);
        }
        else
        {
            session.Versions.ClearVersion<TDoc, TId>(id);
        }
    }

    public sealed override void Store(IStorageSession session, TDoc document, long revision)
    {
        refuseNumericRevisionsIfUnsupported();

        var id = store(session, document);

        if (revision != 0)
        {
            session.Versions.StoreRevision<TDoc, TId>(id, revision);
        }
        else
        {
            session.Versions.ClearRevision<TDoc, TId>(id);
        }
    }

    private TId store(IStorageSession session, TDoc document)
    {
        var id = AssignIdentity(document, session.TenantId, session.Database);
        session.MarkAsAddedForStorage(id, document);

        if (session.ItemMap.TryGetValue(typeof(TDoc), out var items))
        {
            if (items is not Dictionary<TId, TDoc> map)
            {
                throw new IdentityMapIdTypeMismatchException(typeof(TDoc), typeof(TId));
            }

            // Two instances of one document in one session, both stored, is a last-write-wins
            // outcome indistinguishable from a lost update. A document type that declares
            // IEquatable<T> has said what "the same document" means, so it is taken at its word.
            if (map.TryGetValue(id, out var existing) && document is not IEquatable<TDoc>
                                                     && !ReferenceEquals(existing, document))
            {
                throw new InvalidOperationException(
                    $"A different instance of '{typeof(TDoc).FullName}' with identity '{id}' is already in "
                    + "this session's identity map. Store the instance the session handed you, or call "
                    + "Eject to drop the mapped one first.");
            }

            map[id] = document;
        }
        else
        {
            session.ItemMap.Add(typeof(TDoc), new Dictionary<TId, TDoc> { { id, document } });
        }

        return id;
    }

    public sealed override Task<TDoc?> LoadAsync(TId id, IStorageSession session, CancellationToken token)
    {
        if (session.ItemMap.TryGetValue(typeof(TDoc), out var items))
        {
            if (items is not Dictionary<TId, TDoc> map)
            {
                throw new IdentityMapIdTypeMismatchException(typeof(TDoc), typeof(TId));
            }

            if (map.TryGetValue(id, out var cached))
            {
                return Task.FromResult<TDoc?>(cached);
            }
        }

        // The identity-map selector maps what it reads.
        return queryOneAsync(id, session, token);
    }

    public sealed override async Task<IReadOnlyList<TDoc>> LoadManyAsync(TId[] ids, IStorageSession session,
        CancellationToken token)
    {
        Dictionary<TId, TDoc> map;
        if (session.ItemMap.TryGetValue(typeof(TDoc), out var items))
        {
            map = items as Dictionary<TId, TDoc>
                  ?? throw new IdentityMapIdTypeMismatchException(typeof(TDoc), typeof(TId));
        }
        else
        {
            map = new Dictionary<TId, TDoc>();
            session.ItemMap.Add(typeof(TDoc), map);
        }

        // Mapped documents first, then the ones read.
        var list = new List<TDoc>();
        var missing = new List<TId>();
        foreach (var id in ids)
        {
            if (map.TryGetValue(id, out var cached))
            {
                list.Add(cached);
            }
            else
            {
                missing.Add(id);
            }
        }

        // Every id answered from the map: no round trip that could only return no rows.
        if (missing.Count == 0)
        {
            return list;
        }

        await queryManyAsync(missing.ToArray(), session, list, token).ConfigureAwait(false);
        return list;
    }
}

/// <summary>
///     Dirty-tracking storage: the identity map plus a change tracker per loaded document, which the
///     dirty-tracking selectors register.
/// </summary>
internal abstract class DirtyTrackingClosedShapeDocumentStorage<TDoc, TId>: IdentityMapClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    protected DirtyTrackingClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor,
        IClosedShapeStorageRules<TDoc, TId> rules)
        : base(ClosedShapeTrackingMode.DirtyTracking, descriptor, rules)
    {
    }
}
