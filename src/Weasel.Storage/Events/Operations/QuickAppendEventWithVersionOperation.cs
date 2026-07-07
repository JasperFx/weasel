#nullable enable
using System;
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// Closed-shape per-event INSERT operation for the Quick paths' "with version"
/// branch — one INSERT per event with a pre-assigned <see cref="IEvent.Version"/>.
/// Relocated from Marten's <c>Marten.EventStorage.Quick.QuickAppendEventWithVersionOperation</c>
/// (event E3). The Quick appender uses this shape when a stream is starting (no
/// prior version to fetch) or an existing stream has
/// <see cref="StreamAction.ExpectedVersionOnServer"/> set; other cases use the
/// dialect-supplied bulk append operation. Also reused by the Rich storage's
/// side-effect replay path (raised events).
/// </summary>
/// <remarks>
/// Same INSERT shape as <c>RichAppendEventOperation</c>, with one divergence:
/// <c>seq_id</c> may be a server-side <c>nextval(...)</c> SQL literal baked into
/// the supplied suffix instead of a bound parameter, in which case the supplied
/// binder array excludes the sequence binder. The SQL prefix/suffix + binder set
/// are passed in by the calling storage class so this single operation covers all
/// three Quick/QWST/Rich-replay variants.
/// </remarks>
internal sealed class QuickAppendEventWithVersionOperation: AppendEventOperationBase
{
    private readonly string _appendEventSqlPrefix;
    private readonly string _appendEventSqlSuffix;
    private readonly IEventMetadataBinder[] _metadataBinders;
    private readonly bool _isGuidStreamIdentity;
    private readonly Func<IEvent, string> _serializeEventData;
    private readonly Func<IEvent, byte[]?> _serializeEventBdata;
    private readonly IStorageDialect _dialect;

    public QuickAppendEventWithVersionOperation(
        string appendEventSqlPrefix,
        string appendEventSqlSuffix,
        IEventMetadataBinder[] metadataBinders,
        bool isGuidStreamIdentity,
        Func<IEvent, string> serializeEventData,
        Func<IEvent, byte[]?> serializeEventBdata,
        IStorageDialect dialect,
        StreamAction stream,
        IEvent e)
        : base(stream, e)
    {
        _appendEventSqlPrefix = appendEventSqlPrefix;
        _appendEventSqlSuffix = appendEventSqlSuffix;
        _metadataBinders = metadataBinders;
        _isGuidStreamIdentity = isGuidStreamIdentity;
        _serializeEventData = serializeEventData;
        _serializeEventBdata = serializeEventBdata;
        _dialect = dialect;
    }

    public override void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
    {
        builder.Append(_appendEventSqlPrefix);

        var dialect = _dialect;
        IGroupedParameterBuilder pb = builder.CreateGroupedParameterBuilder(',');

        // Core columns — same order + types as RichAppendEventOperation. Provider
        // parameter types come from the dialect so the op carries no direct
        // provider reference.
        dialect.SetParameterType(pb.AppendParameter(_serializeEventData(Event)), StorageColumnType.Json);
        dialect.SetParameterType(pb.AppendParameter(Event.EventTypeName), StorageColumnType.String);
        dialect.SetParameterType(pb.AppendParameter(Event.DotNetTypeName), StorageColumnType.String);

        // bdata bytea (nullable). NULL for JSON-serialized events; bytes for
        // binary-serialized events. The neutral AppendParameter writes a null
        // byte[] as DBNull.
        var bdataBytes = _serializeEventBdata(Event);
        dialect.SetParameterType(pb.AppendParameter(bdataBytes), StorageColumnType.Binary);

        dialect.SetParameterType(pb.AppendParameter(Event.Id), StorageColumnType.Guid);

        if (_isGuidStreamIdentity)
            dialect.SetParameterType(pb.AppendParameter(Stream.Id), StorageColumnType.Guid);
        else
            dialect.SetParameterType(pb.AppendParameter(Stream.Key), StorageColumnType.String);

        dialect.SetParameterType(pb.AppendParameter(Event.Version), StorageColumnType.Long);
        dialect.SetParameterType(pb.AppendParameter(Event.Timestamp), StorageColumnType.Timestamp);
        dialect.SetParameterType(pb.AppendParameter(Stream.TenantId), StorageColumnType.String);

        // Optional metadata binders (causation / correlation / headers /
        // user_name). The filtered list excludes SequenceColumnBinder when seq_id
        // is server-set via the nextval() literal in the suffix.
        for (var i = 0; i < _metadataBinders.Length; i++)
        {
            _metadataBinders[i].Bind(pb, Stream, Event, session);
        }

        builder.Append(_appendEventSqlSuffix);
    }
}
