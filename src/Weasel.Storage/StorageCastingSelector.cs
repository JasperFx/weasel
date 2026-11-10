#nullable enable
using System.Data.Common;

namespace Weasel.Storage;

/// <summary>
///     Narrows a selector over a hierarchy's root type to one of its sub-classes: the row is
///     materialized by the root's selector as whatever its discriminator says, and the result is
///     cast down to <typeparamref name="T" />.
/// </summary>
/// <remarks>
///     <para>
///         What a sub-class storage hands out when it wraps its root's storage rather than being built
///         from its own mapping: the root's selectors only type-check against the root. The narrowing
///         itself happens before the row is read (a discriminator predicate on a query, a type test on a
///         load), so the cast here is expected to succeed.
///     </para>
///     <para>
///         Named with the <c>Storage</c> prefix because Marten, Polecat and Fisher each carry an internal
///         <c>CastingSelector</c>, and a file importing both namespaces would otherwise see CS0104.
///     </para>
/// </remarks>
/// <typeparam name="T">The sub-class the caller asked for.</typeparam>
/// <typeparam name="TRoot">The hierarchy's root document type.</typeparam>
public sealed class StorageCastingSelector<T, TRoot>: ISelector<T> where T : TRoot where TRoot : notnull
{
    private readonly ISelector<TRoot> _inner;

    /// <param name="inner">The root type's selector.</param>
    public StorageCastingSelector(ISelector<TRoot> inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    /// <inheritdoc />
    public T Resolve(DbDataReader reader) => (T)_inner.Resolve(reader)!;

    /// <inheritdoc />
    public async Task<T> ResolveAsync(DbDataReader reader, CancellationToken token)
        => (T)(await _inner.ResolveAsync(reader, token).ConfigureAwait(false))!;
}
