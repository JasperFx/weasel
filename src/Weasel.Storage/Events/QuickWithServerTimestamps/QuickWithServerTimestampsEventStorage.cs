#nullable enable
using JasperFx.Events;

namespace Weasel.Storage;

/// <summary>
/// <see cref="EventStorage{TId}"/> for
/// <c>EventAppendMode.QuickWithServerTimestamps</c>. Same shape as
/// <see cref="QuickEventStorage{TId}"/> + the server-side <c>now()</c> timestamp
/// array; the returned timestamps are written back onto each event in the bulk
/// operation's <c>Postprocess</c>. Relocated from Marten (event E3).
/// </summary>
internal sealed class QuickWithServerTimestampsEventStorage<TId>: EventStorage<TId>
{
    private readonly QuickWithServerTimestampsEventStorageDescriptor _descriptor;

    public QuickWithServerTimestampsEventStorage(QuickWithServerTimestampsEventStorageDescriptor descriptor)
    {
        _descriptor = descriptor;
    }

    public override IStorageOperation AppendEvent(IStorageSession session, StreamAction stream, IEvent @event)
        // Full-mode per-event INSERT for the tombstone / direct-AppendEvent code
        // paths that bypass the bulk function call.
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
        // Dialect-supplied — see QuickEventStorage<TId>.QuickAppendEvents.
        => _descriptor.CreateQuickAppendEventsOperation(_descriptor, stream);

    public override IStorageOperation InsertStream(StreamAction stream)
        => new QuickWithServerTimestampsInsertStreamOperation(_descriptor, stream);

    public override IStorageOperation UpdateStreamVersion(StreamAction stream)
        => new QuickWithServerTimestampsUpdateStreamVersionOperation(_descriptor, stream);

    public override IStorageOperation AssertStreamVersion(StreamAction stream)
        => _descriptor.IsTenancyConjoined
            ? new ConjoinedAssertStreamVersionOperation<TId>(_descriptor.AssertStreamVersionSql, stream)
            : new SingleTenantAssertStreamVersionOperation<TId>(_descriptor.AssertStreamVersionSql, stream);
}
