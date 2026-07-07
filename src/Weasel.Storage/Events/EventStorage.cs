#nullable enable
using JasperFx.Events;

namespace Weasel.Storage;

/// <summary>
/// Dialect-neutral closed-shape event-storage abstraction. Three concrete
/// implementations ship — one per <see cref="EventAppendMode"/> flavor — and
/// exactly one is wired into a store at construction time by
/// <see cref="EventStorageBuilder"/>. Per-call dispatch is a virtual call
/// through this base; no runtime branching on append mode. Relocated from
/// Marten's <c>Marten.EventStorage.EventStorage&lt;TId&gt;</c> as the final
/// slice of the marten#4821 event-storage move (event E3); the same hierarchy
/// now serves Marten (Postgres) and Polecat (SQL Server) via their own
/// <see cref="IEventStoreSqlDialect"/> implementations.
/// </summary>
/// <remarks>
/// Write-only after Marten's E0 prework hoisted the stream-state query handler
/// off this hierarchy. Each concrete subclass implements only the methods
/// appropriate to its append mode; the "wrong-mode" methods throw
/// <see cref="System.NotSupportedException"/> but are never reached because the
/// builder's subclass pick and the session's dispatch are both gated by the
/// same append-mode setting.
/// </remarks>
public abstract class EventStorage<TId>
{
    /// <summary>
    /// Per-event append for the Full mode. One <see cref="IStorageOperation"/>
    /// per event; the session enqueues N operations for an N-event stream.
    /// </summary>
    public abstract IStorageOperation AppendEvent(
        IStorageSession session, StreamAction stream, IEvent @event);

    /// <summary>
    /// Per-event append for the QuickWithVersion mode — same per-event shape as
    /// <see cref="AppendEvent"/>, but the event's version is pre-assigned by the
    /// caller rather than computed from a stream-state lookup.
    /// </summary>
    public abstract IStorageOperation QuickAppendEventWithVersion(StreamAction stream, IEvent @event);

    /// <summary>
    /// Batched per-stream append. One <see cref="IStorageOperation"/> per stream;
    /// the operation is dialect-supplied (Postgres binds array parameters to a
    /// bulk function; another dialect may use a different SQL shape).
    /// </summary>
    public abstract IStorageOperation QuickAppendEvents(StreamAction stream);

    /// <summary>Inserts the <c>mt_streams</c> row when a new stream is opened.</summary>
    public abstract IStorageOperation InsertStream(StreamAction stream);

    /// <summary>Increments the <c>mt_streams</c> version with an expected-version guard.</summary>
    public abstract IStorageOperation UpdateStreamVersion(StreamAction stream);

    /// <summary>
    /// Asserts the expected <c>mt_streams</c> version without appending events
    /// (the <c>AlwaysEnforceConsistency</c> zero-events path). The concrete
    /// storage selects the single-tenant or conjoined operation variant once,
    /// from its descriptor's tenancy style.
    /// </summary>
    public abstract IStorageOperation AssertStreamVersion(StreamAction stream);
}
