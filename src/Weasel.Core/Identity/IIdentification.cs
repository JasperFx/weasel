using System;
using System.Data.Common;
using Weasel.Core.Sequences;

namespace Weasel.Core.Identity;

/// <summary>
///     Non-generic facade over <see cref="IIdentification{TDoc,TId}" />, for callers whose document
///     metadata is <see cref="Type" />-based and therefore cannot name <c>TDoc</c> / <c>TId</c> at
///     compile time — Marten's <c>ProviderGraph</c>, Polecat's <c>DocumentMapping</c>.
/// </summary>
/// <remarks>
///     <para>
///     This exists for Native AOT (weasel#689). Without it, a <see cref="Type" />-keyed consumer has
///     to close a generic over <c>(documentType, idType)</c> at runtime to <em>use</em> a strategy —
///     and <c>Type.MakeGenericType</c> can only close an instantiation whose arguments are all
///     reference types, because those share one canonical body. An id type is routinely
///     <see cref="Guid" />, <see cref="int" /> or <see cref="long" />, so the closure needs code ILC
///     never generated:
///     </para>
///     <code>
///     System.NotSupportedException: 'IdentityAssigner`2[DeadLetterEvent,System.Guid]'
///     is missing native code or metadata.
///     </code>
///     <para>
///     Reflection is not a way out of that either. Holding the strategy as <see cref="object" /> and
///     invoking <c>AssignIfMissing</c> through <c>MethodInfo</c> fails differently — the trimmer
///     removed the member metadata, because nothing statically references it, and
///     <c>GetMethods()</c> comes back empty. The seam has to be a statically-referenced interface.
///     </para>
///     <para>
///     Every member is boxed by construction, so a caller that <em>can</em> name its type arguments
///     should keep using <see cref="IIdentification{TDoc,TId}" /> — the generic path allocates
///     nothing. This facade is the opt-in cost of not knowing the types.
///     </para>
/// </remarks>
public interface IIdentification
{
    /// <summary>
    ///     Boxing equivalent of <see cref="IIdentification{TDoc,TId}.Identity" />.
    /// </summary>
    /// <param name="document">
    ///     Must be an instance of the strategy's document type; the implementation casts.
    /// </param>
    object Identity(object document);

    /// <summary>
    ///     Boxing equivalent of <see cref="IIdentification{TDoc,TId}.AssignIfMissing" />, and the
    ///     member this interface exists for. Mutates <paramref name="document" /> in place exactly as
    ///     the generic call does, and returns the id — existing or newly generated.
    /// </summary>
    object AssignIfMissing(object document, ISequenceSource sequences);

    /// <summary>
    ///     Boxing equivalent of <see cref="IIdentification{TDoc,TId}.ToRawSqlValue" />.
    /// </summary>
    object ToRawSqlValue(object id);

    /// <summary>
    ///     The .NET type matching <see cref="ToRawSqlValue" /> — the id type itself for primitives,
    ///     the inner primitive for strong-typed wrappers.
    /// </summary>
    Type RawSqlType { get; }

    /// <summary>
    ///     Boxing equivalent of <see cref="IIdentification{TDoc,TId}.ReadIdFromReader" />.
    /// </summary>
    object ReadIdFromReader(DbDataReader reader, int columnOrdinal);
}

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
///     <para>
///     The non-generic <see cref="IIdentification" /> base is satisfied here by default
///     implementations that forward to the generic members, so <strong>no existing strategy — in
///     this repository or outside it — changes at all</strong>. A type already compiled against the
///     generic interface alone still loads: the runtime finds the facade's implementations on this
///     interface.
///     </para>
/// </remarks>
public interface IIdentification<TDoc, TId> : IIdentification
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
    new Type RawSqlType => typeof(TId);

    /// <summary>
    ///     Read an id from a result-row column. For primitive id types this is a simple
    ///     <c>reader.GetFieldValue&lt;TId&gt;</c>; strong-typed wrappers read the inner primitive and
    ///     wrap it (the ADO provider can't materialize the wrapper type directly).
    /// </summary>
    new TId ReadIdFromReader(DbDataReader reader, int columnOrdinal)
        => reader.GetFieldValue<TId>(columnOrdinal);

    // The facade, forwarding to the generic members above. Written out rather than left abstract so
    // that adding the base interface is not a breaking change -- see the remarks on IIdentification.
    object IIdentification.Identity(object document) => Identity((TDoc)document)!;

    object IIdentification.AssignIfMissing(object document, ISequenceSource sequences)
        => AssignIfMissing((TDoc)document, sequences)!;

    object IIdentification.ToRawSqlValue(object id) => ToRawSqlValue((TId)id);

    Type IIdentification.RawSqlType => RawSqlType;

    object IIdentification.ReadIdFromReader(DbDataReader reader, int columnOrdinal)
        => ReadIdFromReader(reader, columnOrdinal)!;
}
