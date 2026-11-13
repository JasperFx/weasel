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

    /// <summary>
    ///     The storage for a hierarchy sub-class in one tracking mode: <paramref name="root" />, the
    ///     root type's storage in that mode, wrapped so its reads narrow to <typeparamref name="TDoc" />
    ///     (weasel#811). A store that needs its own interfaces on the sub-class storage derives
    ///     <see cref="ClosedShapeSubClassDocumentStorage{TDoc,TRoot,TId}" /> and constructs it instead.
    /// </summary>
    /// <remarks>
    ///     Like every method here this closes no generic type itself. <typeparamref name="TDoc" /> and
    ///     <typeparamref name="TRoot" /> are document types and so reference types, but a value-type
    ///     <typeparamref name="TId" /> still cannot be closed with <c>MakeGenericMethod</c> under Native
    ///     AOT; a caller holding the types as <see cref="Type" />s has to reach this from a context where
    ///     the id type is already a type argument.
    /// </remarks>
    public static IDocumentStorage<TDoc, TId> BuildSubClassStorage<TDoc, TRoot, TId>(IDocumentStorage<TRoot, TId> root,
        IClosedShapeSubClassRules<TDoc, TRoot, TId> rules)
        where TDoc : notnull, TRoot
        where TRoot : notnull
        where TId : notnull
        => new ClosedShapeSubClassDocumentStorage<TDoc, TRoot, TId>(root, rules);

    /// <summary>
    ///     All four tracking modes of a hierarchy sub-class, each wrapping the root's storage in the same
    ///     mode (weasel#811). One rules object serves all four.
    /// </summary>
    /// <exception cref="ArgumentException">
    ///     A slot of <paramref name="root" /> is not an <see cref="IDocumentStorage{T,TId}" /> keyed by
    ///     <typeparamref name="TId" />.
    /// </exception>
    public static DocumentProvider<TDoc> BuildSubClass<TDoc, TRoot, TId>(DocumentProvider<TRoot> root,
        IClosedShapeSubClassRules<TDoc, TRoot, TId> rules)
        where TDoc : notnull, TRoot
        where TRoot : notnull
        where TId : notnull
    {
        ArgumentNullException.ThrowIfNull(root);
        ArgumentNullException.ThrowIfNull(rules);

        return new DocumentProvider<TDoc>(
            BuildSubClassStorage(keyed(root.QueryOnly), rules),
            BuildSubClassStorage(keyed(root.Lightweight), rules),
            BuildSubClassStorage(keyed(root.IdentityMap), rules),
            BuildSubClassStorage(keyed(root.DirtyTracking), rules));

        static IDocumentStorage<TRoot, TId> keyed(IDocumentStorage<TRoot> storage)
            => storage as IDocumentStorage<TRoot, TId>
               ?? throw new ArgumentException(
                   $"The {typeof(TRoot).Name} storage {storage.GetType().Name} is keyed by {storage.IdType.Name}, not {typeof(TId).Name}.",
                   nameof(root));
    }
}
