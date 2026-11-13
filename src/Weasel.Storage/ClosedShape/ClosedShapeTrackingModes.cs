#nullable enable
namespace Weasel.Storage;

// The four tracking-mode bases of the closed-shape family. The identity-map semantics are Marten's,
// as decided for weasel#740: a second instance under a mapped id is refused (an IEquatable<T>
// document is taken at its word), an identity map keyed by another id type is refused, a revision
// is recorded on Store and a revision of zero clears it, and a nested session can share the
// parent's identity map.

/// <summary>Read-only storage: writes are refused, Store/Eject do nothing.</summary>
/// <remarks>
///     Public and unsealed so a store can derive it to carry its own storage interfaces (weasel#811);
///     every member it overrides is sealed, because a query-only storage refusing writes is the
///     contract, not a default. <see cref="ClosedShapeDocumentProviderFactory" /> builds this class
///     itself for the query-only slot.
/// </remarks>
/// <typeparam name="TDoc">The document type.</typeparam>
/// <typeparam name="TId">The document's id type.</typeparam>
public class QueryOnlyClosedShapeDocumentStorage<TDoc, TId>: ClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    /// <summary>Build the query-only storage.</summary>
    /// <param name="descriptor">Identity, SQL typing and binders for the document type.</param>
    /// <param name="rules">Everything about the document type's storage that differs from store to store.</param>
    public QueryOnlyClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor,
        IClosedShapeStorageRules<TDoc, TId> rules)
        : base(ClosedShapeTrackingMode.QueryOnly, descriptor, rules)
    {
    }

    public sealed override void Store(IStorageSession session, TDoc document)
    {
    }

    public sealed override void Store(IStorageSession session, TDoc document, Guid? version)
    {
    }

    public sealed override void Store(IStorageSession session, TDoc document, long revision)
    {
    }

    public sealed override void Eject(IStorageSession session, TDoc document)
    {
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

    // The query-only read layout has no id column, so the session-free projection loader, which
    // reads the writeable layout, cannot run over it. Projection storage never holds a query-only
    // storage.
    public sealed override Task<TDoc?> LoadProjectedAsync(TId id, IStorageDatabase database, string tenantId,
        CancellationToken token)
        => throw refused(nameof(LoadProjectedAsync));

    public sealed override Task<IReadOnlyList<TDoc>> LoadManyProjectedAsync(TId[] ids, IStorageDatabase database,
        string tenantId, CancellationToken token)
        => throw refused(nameof(LoadManyProjectedAsync));

    public sealed override IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => throw refused(nameof(Insert));

    public sealed override IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => throw refused(nameof(Update));

    public sealed override IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => throw refused(nameof(Upsert));

    public sealed override IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => throw refused(nameof(Overwrite));

    public sealed override IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => throw refused(nameof(OverwriteProjected));

    public sealed override IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => throw refused(nameof(UpsertProjected));

    public sealed override IStorageOperation InsertProjected(TDoc document, string tenantId)
        => throw refused(nameof(InsertProjected));

    public sealed override IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => throw refused(nameof(UpdateProjected));

    public sealed override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalClosedShapeQueryOnlySelector<TDoc, TId>(session, _descriptor)
            : new FlatClosedShapeQueryOnlySelector<TDoc, TId>(session, _descriptor);

    private static NotSupportedException refused(string member)
        => new($"Query-only storage for {typeof(TDoc).Name} does not support {member}.");
}

/// <summary>
///     Lightweight storage: no identity map, versions captured for the optimistic and numeric guards.
/// </summary>
/// <remarks>
///     Public so a store can derive it (weasel#811). A derived class needs only a constructor: the
///     write operations come from <see cref="ClosedShapeDocumentStorage{TDoc,TId}" /> and the selector
///     from <see cref="BuildSelector" />, both for the descriptor's <see cref="ConcurrencyMode" />.
///     Store, Eject and the loads are sealed: they are the tracking mode.
/// </remarks>
/// <typeparam name="TDoc">The document type.</typeparam>
/// <typeparam name="TId">The document's id type.</typeparam>
public abstract class LightweightClosedShapeDocumentStorage<TDoc, TId>: ClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    /// <summary>Build the lightweight storage.</summary>
    /// <param name="descriptor">Identity, SQL typing, write SQL and binders for the document type.</param>
    /// <param name="rules">Everything about the document type's storage that differs from store to store.</param>
    protected LightweightClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor,
        IClosedShapeStorageRules<TDoc, TId> rules)
        : base(ClosedShapeTrackingMode.Lightweight, descriptor, rules)
    {
    }

    /// <summary>
    ///     The lightweight selector for the descriptor's <see cref="ConcurrencyMode" />, or the one that
    ///     captures no versions for a session shared by parallel projection workers
    ///     (<see cref="IClosedShapeStorageRules{TDoc,TId}.IsSharedProjectionSession" />).
    /// </summary>
    public override ISelector BuildSelector(IStorageSession session)
    {
        if (sessionFreeSelector(session) is { } sessionFree)
        {
            return sessionFree;
        }

        var hierarchical = _descriptor.ResolveDocumentType is not null;
        return _descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Off => hierarchical
                ? new HierarchicalUnversionedClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor)
                : new FlatUnversionedClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor),
            ConcurrencyMode.Optimistic => hierarchical
                ? new HierarchicalOptimisticClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor)
                : new FlatOptimisticClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor),
            ConcurrencyMode.Numeric => hierarchical
                ? new HierarchicalNumericClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor)
                : new FlatNumericClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor),
            _ => throw unknownConcurrencyMode()
        };
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
    private protected ISelector? sessionFreeSelector(IStorageSession session)
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
/// <remarks>
///     Public so a store can derive it (weasel#811); a derived class needs only a constructor. The
///     identity-map semantics are sealed, and they are Marten's (weasel#740): a different instance under
///     a mapped id is refused unless the document is <see cref="IEquatable{T}" />, and an identity map
///     keyed by another id type is refused with <see cref="IdentityMapIdTypeMismatchException" />.
/// </remarks>
/// <typeparam name="TDoc">The document type.</typeparam>
/// <typeparam name="TId">The document's id type.</typeparam>
public abstract class IdentityMapClosedShapeDocumentStorage<TDoc, TId>: ClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    /// <summary>Build the identity-map storage.</summary>
    /// <param name="descriptor">Identity, SQL typing, write SQL and binders for the document type.</param>
    /// <param name="rules">Everything about the document type's storage that differs from store to store.</param>
    protected IdentityMapClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor,
        IClosedShapeStorageRules<TDoc, TId> rules)
        : this(ClosedShapeTrackingMode.IdentityMap, descriptor, rules)
    {
    }

    private protected IdentityMapClosedShapeDocumentStorage(ClosedShapeTrackingMode mode,
        DocumentStorageDescriptor<TDoc, TId> descriptor, IClosedShapeStorageRules<TDoc, TId> rules)
        : base(mode, descriptor, rules)
    {
    }

    /// <summary>
    ///     The identity-map selector for the descriptor's <see cref="ConcurrencyMode" />: every row it
    ///     reads is put in the session's identity map.
    /// </summary>
    public override ISelector BuildSelector(IStorageSession session)
    {
        var hierarchical = _descriptor.ResolveDocumentType is not null;
        return _descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Off => hierarchical
                ? new HierarchicalUnversionedClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor)
                : new FlatUnversionedClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor),
            ConcurrencyMode.Optimistic => hierarchical
                ? new HierarchicalOptimisticClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor)
                : new FlatOptimisticClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor),
            ConcurrencyMode.Numeric => hierarchical
                ? new HierarchicalNumericClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor)
                : new FlatNumericClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor),
            _ => throw unknownConcurrencyMode()
        };
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
/// <remarks>Public so a store can derive it (weasel#811); a derived class needs only a constructor.</remarks>
/// <typeparam name="TDoc">The document type.</typeparam>
/// <typeparam name="TId">The document's id type.</typeparam>
public abstract class DirtyTrackingClosedShapeDocumentStorage<TDoc, TId>: IdentityMapClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    /// <summary>Build the dirty-tracking storage.</summary>
    /// <param name="descriptor">Identity, SQL typing, write SQL and binders for the document type.</param>
    /// <param name="rules">Everything about the document type's storage that differs from store to store.</param>
    protected DirtyTrackingClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor,
        IClosedShapeStorageRules<TDoc, TId> rules)
        : base(ClosedShapeTrackingMode.DirtyTracking, descriptor, rules)
    {
    }

    /// <summary>
    ///     The dirty-tracking selector for the descriptor's <see cref="ConcurrencyMode" />: every row it
    ///     reads is put in the identity map and gets a change tracker.
    /// </summary>
    public override ISelector BuildSelector(IStorageSession session)
    {
        var hierarchical = _descriptor.ResolveDocumentType is not null;
        return _descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Off => hierarchical
                ? new HierarchicalUnversionedClosedShapeDirtyTrackingSelector<TDoc, TId>(session, _descriptor)
                : new FlatUnversionedClosedShapeDirtyTrackingSelector<TDoc, TId>(session, _descriptor),
            ConcurrencyMode.Optimistic => hierarchical
                ? new HierarchicalOptimisticClosedShapeDirtyTrackingSelector<TDoc, TId>(session, _descriptor)
                : new FlatOptimisticClosedShapeDirtyTrackingSelector<TDoc, TId>(session, _descriptor),
            ConcurrencyMode.Numeric => hierarchical
                ? new HierarchicalNumericClosedShapeDirtyTrackingSelector<TDoc, TId>(session, _descriptor)
                : new FlatNumericClosedShapeDirtyTrackingSelector<TDoc, TId>(session, _descriptor),
            _ => throw unknownConcurrencyMode()
        };
    }
}
