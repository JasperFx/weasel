#nullable enable
namespace Weasel.Storage;

// The nine sealed leaves: {Lightweight, IdentityMap, DirtyTracking} x {Unversioned, Optimistic,
// Numeric}. Each is monomorphic for its (TDoc, TId, tracking mode, concurrency mode): the write
// factories and the selector family are fixed by the type, so nothing branches on either per call.
// The Flat / Hierarchical selector choice is made once per query, never per row.

internal sealed class UnversionedLightweightClosedShapeDocumentStorage<TDoc, TId>: LightweightClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public UnversionedLightweightClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor, IClosedShapeStorageRules<TDoc, TId> rules)
        : base(descriptor, rules)
    {
    }

    public override IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.Insert(this, document, session, tenantId);

    public override IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.Update(this, document, session, tenantId);

    public override IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.Upsert(this, document, session, tenantId);

    public override IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.Overwrite(this, document, session, tenantId);

    public override IStorageOperation InsertProjected(TDoc document, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.InsertProjected(this, document, tenantId);

    public override IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.UpdateProjected(this, document, tenantId);

    public override IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.UpsertProjected(this, document, tenantId);

    public override IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.OverwriteProjected(this, document, tenantId);

    public override ISelector BuildSelector(IStorageSession session)
        => sessionFreeSelector(session)
           ?? (_descriptor.ResolveDocumentType is not null
               ? new HierarchicalUnversionedClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor)
               : new FlatUnversionedClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor));
}

internal sealed class OptimisticLightweightClosedShapeDocumentStorage<TDoc, TId>: LightweightClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public OptimisticLightweightClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor, IClosedShapeStorageRules<TDoc, TId> rules)
        : base(descriptor, rules)
    {
    }

    public override IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.Insert(this, document, session, tenantId);

    public override IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.Update(this, document, session, tenantId);

    public override IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.Upsert(this, document, session, tenantId);

    public override IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.Overwrite(this, document, session, tenantId);

    public override IStorageOperation InsertProjected(TDoc document, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.InsertProjected(this, document, tenantId);

    public override IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.UpdateProjected(this, document, tenantId);

    public override IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.UpsertProjected(this, document, tenantId);

    public override IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.OverwriteProjected(this, document, tenantId);

    public override ISelector BuildSelector(IStorageSession session)
        => sessionFreeSelector(session)
           ?? (_descriptor.ResolveDocumentType is not null
               ? new HierarchicalOptimisticClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor)
               : new FlatOptimisticClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor));
}

internal sealed class NumericLightweightClosedShapeDocumentStorage<TDoc, TId>: LightweightClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public NumericLightweightClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor, IClosedShapeStorageRules<TDoc, TId> rules)
        : base(descriptor, rules)
    {
    }

    public override IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.Insert(this, document, session, tenantId);

    public override IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.Update(this, document, session, tenantId);

    public override IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.Upsert(this, document, session, tenantId);

    public override IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.Overwrite(this, document, session, tenantId);

    public override IStorageOperation InsertProjected(TDoc document, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.InsertProjected(this, document, tenantId);

    public override IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.UpdateProjected(this, document, tenantId);

    public override IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.UpsertProjected(this, document, tenantId);

    public override IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.OverwriteProjected(this, document, tenantId);

    public override ISelector BuildSelector(IStorageSession session)
        => sessionFreeSelector(session)
           ?? (_descriptor.ResolveDocumentType is not null
               ? new HierarchicalNumericClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor)
               : new FlatNumericClosedShapeLightweightSelector<TDoc, TId>(session, _descriptor));
}

internal sealed class UnversionedIdentityMapClosedShapeDocumentStorage<TDoc, TId>: IdentityMapClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public UnversionedIdentityMapClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor, IClosedShapeStorageRules<TDoc, TId> rules)
        : base(descriptor, rules)
    {
    }

    public override IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.Insert(this, document, session, tenantId);

    public override IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.Update(this, document, session, tenantId);

    public override IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.Upsert(this, document, session, tenantId);

    public override IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.Overwrite(this, document, session, tenantId);

    public override IStorageOperation InsertProjected(TDoc document, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.InsertProjected(this, document, tenantId);

    public override IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.UpdateProjected(this, document, tenantId);

    public override IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.UpsertProjected(this, document, tenantId);

    public override IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.OverwriteProjected(this, document, tenantId);

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalUnversionedClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor)
            : new FlatUnversionedClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor);
}

internal sealed class OptimisticIdentityMapClosedShapeDocumentStorage<TDoc, TId>: IdentityMapClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public OptimisticIdentityMapClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor, IClosedShapeStorageRules<TDoc, TId> rules)
        : base(descriptor, rules)
    {
    }

    public override IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.Insert(this, document, session, tenantId);

    public override IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.Update(this, document, session, tenantId);

    public override IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.Upsert(this, document, session, tenantId);

    public override IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.Overwrite(this, document, session, tenantId);

    public override IStorageOperation InsertProjected(TDoc document, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.InsertProjected(this, document, tenantId);

    public override IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.UpdateProjected(this, document, tenantId);

    public override IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.UpsertProjected(this, document, tenantId);

    public override IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.OverwriteProjected(this, document, tenantId);

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalOptimisticClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor)
            : new FlatOptimisticClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor);
}

internal sealed class NumericIdentityMapClosedShapeDocumentStorage<TDoc, TId>: IdentityMapClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public NumericIdentityMapClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor, IClosedShapeStorageRules<TDoc, TId> rules)
        : base(descriptor, rules)
    {
    }

    public override IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.Insert(this, document, session, tenantId);

    public override IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.Update(this, document, session, tenantId);

    public override IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.Upsert(this, document, session, tenantId);

    public override IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.Overwrite(this, document, session, tenantId);

    public override IStorageOperation InsertProjected(TDoc document, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.InsertProjected(this, document, tenantId);

    public override IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.UpdateProjected(this, document, tenantId);

    public override IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.UpsertProjected(this, document, tenantId);

    public override IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.OverwriteProjected(this, document, tenantId);

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalNumericClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor)
            : new FlatNumericClosedShapeIdentityMapSelector<TDoc, TId>(session, _descriptor);
}

internal sealed class UnversionedDirtyTrackingClosedShapeDocumentStorage<TDoc, TId>: DirtyTrackingClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public UnversionedDirtyTrackingClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor, IClosedShapeStorageRules<TDoc, TId> rules)
        : base(descriptor, rules)
    {
    }

    public override IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.Insert(this, document, session, tenantId);

    public override IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.Update(this, document, session, tenantId);

    public override IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.Upsert(this, document, session, tenantId);

    public override IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.Overwrite(this, document, session, tenantId);

    public override IStorageOperation InsertProjected(TDoc document, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.InsertProjected(this, document, tenantId);

    public override IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.UpdateProjected(this, document, tenantId);

    public override IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.UpsertProjected(this, document, tenantId);

    public override IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => UnversionedClosedShapeWrites<TDoc, TId>.OverwriteProjected(this, document, tenantId);

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalUnversionedClosedShapeDirtyTrackingSelector<TDoc, TId>(session, _descriptor)
            : new FlatUnversionedClosedShapeDirtyTrackingSelector<TDoc, TId>(session, _descriptor);
}

internal sealed class OptimisticDirtyTrackingClosedShapeDocumentStorage<TDoc, TId>: DirtyTrackingClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public OptimisticDirtyTrackingClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor, IClosedShapeStorageRules<TDoc, TId> rules)
        : base(descriptor, rules)
    {
    }

    public override IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.Insert(this, document, session, tenantId);

    public override IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.Update(this, document, session, tenantId);

    public override IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.Upsert(this, document, session, tenantId);

    public override IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.Overwrite(this, document, session, tenantId);

    public override IStorageOperation InsertProjected(TDoc document, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.InsertProjected(this, document, tenantId);

    public override IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.UpdateProjected(this, document, tenantId);

    public override IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.UpsertProjected(this, document, tenantId);

    public override IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => OptimisticClosedShapeWrites<TDoc, TId>.OverwriteProjected(this, document, tenantId);

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalOptimisticClosedShapeDirtyTrackingSelector<TDoc, TId>(session, _descriptor)
            : new FlatOptimisticClosedShapeDirtyTrackingSelector<TDoc, TId>(session, _descriptor);
}

internal sealed class NumericDirtyTrackingClosedShapeDocumentStorage<TDoc, TId>: DirtyTrackingClosedShapeDocumentStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public NumericDirtyTrackingClosedShapeDocumentStorage(DocumentStorageDescriptor<TDoc, TId> descriptor, IClosedShapeStorageRules<TDoc, TId> rules)
        : base(descriptor, rules)
    {
    }

    public override IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.Insert(this, document, session, tenantId);

    public override IStorageOperation Update(TDoc document, IStorageSession session, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.Update(this, document, session, tenantId);

    public override IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.Upsert(this, document, session, tenantId);

    public override IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.Overwrite(this, document, session, tenantId);

    public override IStorageOperation InsertProjected(TDoc document, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.InsertProjected(this, document, tenantId);

    public override IStorageOperation UpdateProjected(TDoc document, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.UpdateProjected(this, document, tenantId);

    public override IStorageOperation UpsertProjected(TDoc document, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.UpsertProjected(this, document, tenantId);

    public override IStorageOperation OverwriteProjected(TDoc document, string tenantId)
        => NumericClosedShapeWrites<TDoc, TId>.OverwriteProjected(this, document, tenantId);

    public override ISelector BuildSelector(IStorageSession session)
        => _descriptor.ResolveDocumentType is not null
            ? new HierarchicalNumericClosedShapeDirtyTrackingSelector<TDoc, TId>(session, _descriptor)
            : new FlatNumericClosedShapeDirtyTrackingSelector<TDoc, TId>(session, _descriptor);
}
