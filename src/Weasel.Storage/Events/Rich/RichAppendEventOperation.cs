#nullable enable
using System;
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
/// <remarks>
/// Disposable since weasel#831: with <see cref="RichEventStorageDescriptor.WriteEventBdata"/> set, the
/// <c>bdata</c> parameter is a slice of a pooled buffer this operation holds until it is disposed, after
/// the unit of work.
/// </remarks>
internal sealed class RichAppendEventOperation: AppendEventOperationBase, IDisposable
{
    private readonly RichEventStorageDescriptor _descriptor;
    private EventBdataParameter _bdata;

    public RichAppendEventOperation(RichEventStorageDescriptor descriptor, StreamAction stream, IEvent e)
        : base(stream, e, descriptor.TransformAppendEventException)
    {
        _descriptor = descriptor;
        _bdata = new EventBdataParameter(descriptor.SerializeEventBdata, descriptor.WriteEventBdata);
    }

    public void Dispose() => _bdata.Release();

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
        // binary-serialized events -- from a pooled buffer when the store supplies
        // WriteEventBdata (weasel#831).
        _bdata.Append(pb, dialect, Event);

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
