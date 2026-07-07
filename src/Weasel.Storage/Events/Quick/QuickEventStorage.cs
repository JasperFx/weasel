#nullable enable
using JasperFx.Events;

namespace Weasel.Storage;

/// <summary>
/// <see cref="EventStorage{TId}"/> for <c>EventAppendMode.Quick</c> — batch
/// append via a dialect-supplied bulk operation covering every event in the
/// stream. Relocated from Marten (event E3).
/// </summary>
internal sealed class QuickEventStorage<TId>: EventStorage<TId>
{
    private readonly QuickEventStorageDescriptor _descriptor;

    public QuickEventStorage(QuickEventStorageDescriptor descriptor)
    {
        _descriptor = descriptor;
    }

    public override IStorageOperation AppendEvent(IStorageSession session, StreamAction stream, IEvent @event)
        // Full-mode per-event INSERT — seq_id is a bound parameter. The tombstone
        // batch (and a few other code paths) call AppendEvent directly regardless
        // of append mode, with sequences pre-assigned on @event.Sequence.
        => new QuickAppendEventWithVersionOperation(
            _descriptor.AppendEventSqlPrefix,
            _descriptor.AppendEventFullSqlSuffix,
            _descriptor.AppendEventFullMetadataBinders,
            _descriptor.IsGuidStreamIdentity,
            _descriptor.SerializeEventData,
            _descriptor.SerializeEventBdata,
            _descriptor.Dialect,
            stream,
            @event);

    public override IStorageOperation QuickAppendEventWithVersion(StreamAction stream, IEvent @event)
        => new QuickAppendEventWithVersionOperation(
            _descriptor.AppendEventSqlPrefix,
            _descriptor.AppendEventSqlSuffix,
            _descriptor.MetadataBinders,
            _descriptor.IsGuidStreamIdentity,
            _descriptor.SerializeEventData,
            _descriptor.SerializeEventBdata,
            _descriptor.Dialect,
            stream,
            @event);

    public override IStorageOperation QuickAppendEvents(StreamAction stream)
        // Dialect-supplied — the batched append shape is inherently dialect-specific
        // (Postgres binds array parameters to mt_quick_append_events; another dialect
        // may use a multi-row INSERT + OUTPUT), so the operation's construction lives
        // on the descriptor. The factory returns the neutral IStorageOperation, so no
        // cast is needed here.
        => _descriptor.CreateQuickAppendEventsOperation(_descriptor, stream);

    public override IStorageOperation InsertStream(StreamAction stream)
        => new QuickInsertStreamOperation(_descriptor, stream);

    public override IStorageOperation UpdateStreamVersion(StreamAction stream)
        => new QuickUpdateStreamVersionOperation(_descriptor, stream);

    public override IStorageOperation AssertStreamVersion(StreamAction stream)
        => _descriptor.IsTenancyConjoined
            ? new ConjoinedAssertStreamVersionOperation<TId>(_descriptor.AssertStreamVersionSql, stream)
            : new SingleTenantAssertStreamVersionOperation<TId>(_descriptor.AssertStreamVersionSql, stream);
}
