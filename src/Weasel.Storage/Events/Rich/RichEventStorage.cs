#nullable enable
using System;
using JasperFx.Events;

namespace Weasel.Storage;

/// <summary>
/// <see cref="EventStorage{TId}"/> for the Rich (per-event) append paths — covers
/// both <c>EventAppendMode.Rich</c> and <c>EventAppendMode.QuickWithVersion</c>.
/// Yields one <see cref="IStorageOperation"/> per event. Relocated from Marten
/// (event E3).
/// </summary>
internal sealed class RichEventStorage<TId>: EventStorage<TId>
{
    private readonly RichEventStorageDescriptor _descriptor;

    public RichEventStorage(RichEventStorageDescriptor descriptor)
    {
        _descriptor = descriptor;
    }

    public override IStorageOperation AppendEvent(IStorageSession session, StreamAction stream, IEvent @event)
        => new RichAppendEventOperation(_descriptor, stream, @event);

    // Invoked by JasperFx.Events EventSlice.BuildOperations during async
    // projection side-effect replay (raised events) — the caller pre-assigns
    // event.Version but not event.Sequence, so the per-event INSERT uses the
    // server-side seq_id nextval() literal in the suffix. The INSERT shape is
    // identical to the Quick "with version" op, so we reuse it: descriptor SQL
    // strings differ, operation logic doesn't.
    public override IStorageOperation QuickAppendEventWithVersion(StreamAction stream, IEvent @event)
        => new QuickAppendEventWithVersionOperation(
            _descriptor.AppendEventSqlPrefix,
            _descriptor.AppendEventQuickWithVersionSqlSuffix,
            _descriptor.MetadataBindersWithoutSequence,
            _descriptor.IsGuidStreamIdentity,
            _descriptor.SerializeEventData,
            _descriptor.SerializeEventBdata,
            _descriptor.Dialect,
            stream,
            @event);

    public override IStorageOperation QuickAppendEvents(StreamAction stream)
        => throw new NotSupportedException(
            $"{nameof(RichEventStorage<TId>)} doesn't support batch quick-append. " +
            $"Set the append mode to Quick or QuickWithServerTimestamps to use a batched storage class.");

    public override IStorageOperation InsertStream(StreamAction stream)
        => new RichInsertStreamOperation(_descriptor, stream);

    public override IStorageOperation UpdateStreamVersion(StreamAction stream)
        => new RichUpdateStreamVersionOperation(_descriptor, stream);

    public override IStorageOperation AssertStreamVersion(StreamAction stream)
        => _descriptor.IsTenancyConjoined
            ? new ConjoinedAssertStreamVersionOperation<TId>(_descriptor.AssertStreamVersionSql, stream)
            : new SingleTenantAssertStreamVersionOperation<TId>(_descriptor.AssertStreamVersionSql, stream);
}
