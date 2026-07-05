using System;
using System.Data.Common;
using Weasel.Core.Sequences;

namespace Weasel.Core.Identity;

/// <summary>
///     Shared per-document-type identity-strategy contract for closed-shape document storage.
///     One implementation per identity strategy (sequential GUID, random GUID, Hi-Lo int/long,
///     identity-key, externally-assigned string, strong-typed ids) composes with one storage class
///     per <c>(StorageStyle × Concurrency × Hierarchical)</c> tuple — additive rather than
///     combinatorial. Lifted from Marten's <c>Marten.Internal.ClosedShape.IIdentification</c> into
///     Weasel.Core so Marten and Polecat share the identity runtime (JasperFx/polecat#273).
/// </summary>
/// <remarks>
///     Per-call cost is one virtual call into the strategy plus whatever the strategy's body does:
///     a getter-delegate read on the "already has an id" hot path, or a sequence / CombGuid call on
///     the generate path. No allocations when the document already has an id.
/// </remarks>
public interface IIdentification<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    /// <summary>
    ///     Read the current identity from a document instance. Pure — no side effects, no database
    ///     access. Returns the default <typeparamref name="TId" /> value when the document has not been
    ///     assigned an id yet (callers use that to decide whether to generate one).
    /// </summary>
    TId Identity(TDoc document);

    /// <summary>
    ///     Idempotent identity assignment. If the document already has a non-default id, return it
    ///     unchanged (no allocations, no database round-trip). Otherwise generate a new id by the
    ///     strategy's rules (CombGuid / Hi-Lo / identity-key / …), write it onto the document via the
    ///     strategy's setter, and return the new value.
    /// </summary>
    /// <param name="document">The document to inspect and potentially mutate.</param>
    /// <param name="sequences">
    ///     Resolves the Hi-Lo sequence for strategies that need one (Hi-Lo, identity-key, strong-typed
    ///     numeric ids). Strategies that don't (sequential/random GUID, externally-assigned string keys)
    ///     ignore it.
    /// </param>
    TId AssignIfMissing(TDoc document, ISequenceSource sequences);

    /// <summary>
    ///     Convert an id to the value the database should bind — for primitive id types this is the id
    ///     itself; strong-typed wrappers return the inner primitive. Default boxes <paramref name="id" />.
    /// </summary>
    object ToRawSqlValue(TId id) => id!;

    /// <summary>
    ///     The .NET type matching <see cref="ToRawSqlValue" /> — the same as <typeparamref name="TId" />
    ///     for primitives; the inner primitive type for strong-typed wrappers. Dialect operations map
    ///     this to their provider parameter type instead of looking it up from <c>typeof(TId)</c>.
    /// </summary>
    Type RawSqlType => typeof(TId);

    /// <summary>
    ///     Read an id from a result-row column. For primitive id types this is a simple
    ///     <c>reader.GetFieldValue&lt;TId&gt;</c>; strong-typed wrappers read the inner primitive and
    ///     wrap it (the ADO provider can't materialize the wrapper type directly).
    /// </summary>
    TId ReadIdFromReader(DbDataReader reader, int columnOrdinal)
        => reader.GetFieldValue<TId>(columnOrdinal);
}
