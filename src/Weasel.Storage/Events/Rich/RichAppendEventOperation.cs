#nullable enable
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// Closed-shape <see cref="AppendEventOperationBase"/> for the Rich (full-mode)
/// per-event append path. Binds one parameter per column in the order the
/// dialect's <see cref="RichEventStorageDescriptor.AppendEventSqlPrefix"/>
/// declares, then loops the descriptor's metadata-binder array (one virtual
/// <see cref="IEventMetadataBinder.Bind"/> call per active metadata column, in
/// lockstep with the SQL prefix's column order). Relocated from Marten's
/// <c>Marten.EventStorage.Rich.RichAppendEventOperation</c> (event E3).
/// </summary>
internal sealed class RichAppendEventOperation: AppendEventOperationBase
{
    private readonly RichEventStorageDescriptor _descriptor;

    public RichAppendEventOperation(RichEventStorageDescriptor descriptor, StreamAction stream, IEvent e)
        : base(stream, e)
    {
        _descriptor = descriptor;
    }

    public override void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
    {
        builder.Append(_descriptor.AppendEventSqlPrefix);

        var dialect = _descriptor.Dialect;
        IGroupedParameterBuilder pb = builder.CreateGroupedParameterBuilder(',');

        // Core columns, in the dialect's column order. Must match the dialect's
        // BuildAppendEventFullColumnsAndPrefix:
        //   data, type, mt_dotnet_type, bdata, id, stream_id, version, timestamp,
        //   tenant_id, then metadata binders (e.g., seq_id) at the end.
        // Provider parameter types come from the dialect's SetParameterType so the
        // op carries no direct provider reference.
        dialect.SetParameterType(pb.AppendParameter(_descriptor.SerializeEventData(Event)), StorageColumnType.Json);

        dialect.SetParameterType(pb.AppendParameter(Event.EventTypeName), StorageColumnType.String);
        dialect.SetParameterType(pb.AppendParameter(Event.DotNetTypeName), StorageColumnType.String);

        // bdata bytea (nullable): NULL for JSON-serialized events, payload for
        // binary-serialized events. The neutral AppendParameter writes a null
        // byte[] as DBNull.
        var bdataBytes = _descriptor.SerializeEventBdata(Event);
        dialect.SetParameterType(pb.AppendParameter(bdataBytes), StorageColumnType.Binary);

        dialect.SetParameterType(pb.AppendParameter(Event.Id), StorageColumnType.Guid);

        // stream_id — Guid streams use Stream.Id, string streams use Stream.Key.
        // The descriptor flag is set once at startup.
        if (_descriptor.IsGuidStreamIdentity)
        {
            dialect.SetParameterType(pb.AppendParameter(Stream.Id), StorageColumnType.Guid);
        }
        else
        {
            dialect.SetParameterType(pb.AppendParameter(Stream.Key), StorageColumnType.String);
        }

        dialect.SetParameterType(pb.AppendParameter(Event.Version), StorageColumnType.Long);
        dialect.SetParameterType(pb.AppendParameter(Event.Timestamp), StorageColumnType.Timestamp);
        dialect.SetParameterType(pb.AppendParameter(Stream.TenantId), StorageColumnType.String);

        // Metadata binders (seq_id + any optional metadata columns). Order matches
        // the metadata slice of the SQL prefix's column list; the dialect's
        // descriptor builder owns both sides.
        var binders = _descriptor.MetadataBinders;
        for (var i = 0; i < binders.Length; i++)
        {
            binders[i].Bind(pb, Stream, Event, session);
        }

        builder.Append(_descriptor.AppendEventSqlSuffix);
    }
}
