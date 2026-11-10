#nullable enable
namespace Weasel.Storage;

/// <summary>
///     Builds the closed-shape storages for one document type: the leaf for a (tracking mode,
///     concurrency mode) pair, or all four tracking modes as a <see cref="DocumentProvider{T}" />.
///     The leaf is chosen once here, so the storage never branches on either mode per call.
/// </summary>
/// <remarks>
///     The factory closes no generic type itself: every leaf is constructed with the caller's
///     <c>TDoc</c> and <c>TId</c>. A caller that holds the id type only as a <see cref="Type" />
///     has to close <c>BuildStorage&lt;TDoc, TId&gt;</c> itself, and under Native AOT that cannot be
///     done with <c>MakeGenericMethod</c> for a value-type id (see CLAUDE.md, "A Type-keyed seam
///     cannot be a generic").
/// </remarks>
public static class ClosedShapeDocumentProviderFactory
{
    /// <summary>
    ///     The storage for one tracking mode. The concurrency mode comes from
    ///     <see cref="DocumentStorageDescriptor{TDoc,TId}.ConcurrencyMode" />.
    /// </summary>
    public static IClosedShapeDocumentStorage<TDoc, TId> BuildStorage<TDoc, TId>(ClosedShapeTrackingMode mode,
        DocumentStorageDescriptor<TDoc, TId> descriptor, IClosedShapeStorageRules<TDoc, TId> rules)
        where TDoc : notnull
        where TId : notnull
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ArgumentNullException.ThrowIfNull(rules);

        return (mode, descriptor.ConcurrencyMode) switch
        {
            (ClosedShapeTrackingMode.QueryOnly, _) =>
                new QueryOnlyClosedShapeDocumentStorage<TDoc, TId>(descriptor, rules),

            (ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Off) =>
                new UnversionedLightweightClosedShapeDocumentStorage<TDoc, TId>(descriptor, rules),
            (ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Optimistic) =>
                new OptimisticLightweightClosedShapeDocumentStorage<TDoc, TId>(descriptor, rules),
            (ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Numeric) =>
                new NumericLightweightClosedShapeDocumentStorage<TDoc, TId>(descriptor, rules),

            (ClosedShapeTrackingMode.IdentityMap, ConcurrencyMode.Off) =>
                new UnversionedIdentityMapClosedShapeDocumentStorage<TDoc, TId>(descriptor, rules),
            (ClosedShapeTrackingMode.IdentityMap, ConcurrencyMode.Optimistic) =>
                new OptimisticIdentityMapClosedShapeDocumentStorage<TDoc, TId>(descriptor, rules),
            (ClosedShapeTrackingMode.IdentityMap, ConcurrencyMode.Numeric) =>
                new NumericIdentityMapClosedShapeDocumentStorage<TDoc, TId>(descriptor, rules),

            (ClosedShapeTrackingMode.DirtyTracking, ConcurrencyMode.Off) =>
                new UnversionedDirtyTrackingClosedShapeDocumentStorage<TDoc, TId>(descriptor, rules),
            (ClosedShapeTrackingMode.DirtyTracking, ConcurrencyMode.Optimistic) =>
                new OptimisticDirtyTrackingClosedShapeDocumentStorage<TDoc, TId>(descriptor, rules),
            (ClosedShapeTrackingMode.DirtyTracking, ConcurrencyMode.Numeric) =>
                new NumericDirtyTrackingClosedShapeDocumentStorage<TDoc, TId>(descriptor, rules),

            _ => throw new ArgumentOutOfRangeException(nameof(mode),
                $"No closed-shape storage for tracking mode {mode} with concurrency mode {descriptor.ConcurrencyMode}.")
        };
    }

    /// <summary>
    ///     All four tracking modes as a <see cref="DocumentProvider{T}" />. A store whose provider
    ///     derives from <see cref="DocumentProvider{T}" /> (to carry a bulk loader, say) calls
    ///     <see cref="BuildStorage{TDoc,TId}" /> per mode instead.
    /// </summary>
    public static DocumentProvider<TDoc> Build<TDoc, TId>(DocumentStorageDescriptor<TDoc, TId> descriptor,
        IClosedShapeStorageRules<TDoc, TId> rules)
        where TDoc : notnull
        where TId : notnull
        => new(
            BuildStorage(ClosedShapeTrackingMode.QueryOnly, descriptor, rules),
            BuildStorage(ClosedShapeTrackingMode.Lightweight, descriptor, rules),
            BuildStorage(ClosedShapeTrackingMode.IdentityMap, descriptor, rules),
            BuildStorage(ClosedShapeTrackingMode.DirtyTracking, descriptor, rules));
}
