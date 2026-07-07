#nullable enable
using System;
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// Per-event-store-configuration descriptor for the Rich (full-mode) append
/// flow. Holds only the SQL strings + delegates the Rich operations need —
/// nothing about the Quick paths leaks in.
/// </summary>
/// <remarks>
/// Rich-mode SQL is split into a prefix + suffix because the append output
/// writes parameters inline between them, one per core column plus one
/// virtual <see cref="IEventMetadataBinder.Bind"/> call per active metadata
/// binder. See <see cref="MetadataBinders"/> for the metadata hybrid.
/// </remarks>
public sealed class RichEventStorageDescriptor
{
    public RichEventStorageDescriptor(
        string appendEventSqlPrefix,
        string appendEventSqlSuffix,
        string insertStreamSql,
        string updateStreamVersionSql,
        Func<IEvent, string> serializeEventData,
        Func<IEvent, byte[]?> serializeEventBdata,
        IEventMetadataBinder[] metadataBinders)
    {
        AppendEventSqlPrefix = appendEventSqlPrefix;
        AppendEventSqlSuffix = appendEventSqlSuffix;
        InsertStreamSql = insertStreamSql;
        UpdateStreamVersionSql = updateStreamVersionSql;
        SerializeEventData = serializeEventData;
        SerializeEventBdata = serializeEventBdata;
        MetadataBinders = metadataBinders;
    }

    public string AppendEventSqlPrefix { get; }
    public string AppendEventSqlSuffix { get; }
    public string InsertStreamSql { get; }
    public string UpdateStreamVersionSql { get; }

    /// <summary>
    ///     Serializer for the <c>data</c> jsonb column. Returns the full JSON
    ///     payload for JSON-serialized events and the literal <c>{}</c>
    ///     placeholder for binary-serialized events (the real payload lives in
    ///     <c>bdata</c> in that case — see <see cref="SerializeEventBdata"/>).
    /// </summary>
    public Func<IEvent, string> SerializeEventData { get; }

    /// <summary>
    ///     Serializer for the <c>bdata</c> bytea column. Returns the
    ///     serialized bytes for binary-serialized events; returns <c>null</c>
    ///     (bound as <see cref="System.DBNull.Value"/>) for JSON-serialized
    ///     events.
    /// </summary>
    public Func<IEvent, byte[]?> SerializeEventBdata { get; }

    /// <summary>
    /// Ordered metadata-column binders. Rich-mode only — Quick-mode
    /// descriptors don't expose this because Quick's metadata binding is
    /// hand-written inline (per-batch array parameters; no per-event dispatch
    /// worth abstracting).
    /// </summary>
    public IEventMetadataBinder[] MetadataBinders { get; }

    /// <summary>
    /// SQL suffix for the per-event <c>QuickWithVersion</c> path on this
    /// Rich descriptor — same per-event INSERT shape as
    /// <see cref="AppendEventSqlSuffix"/>, but with a server-side
    /// <c>nextval('{schema}.mt_events_sequence')</c> literal in place of the
    /// bound <c>seq_id</c> parameter. Used by the async-projection
    /// side-effect replay path (raised events): the caller pre-assigns
    /// <c>event.Version</c> but not <c>event.Sequence</c>, so the server
    /// claims the sequence inline.
    /// </summary>
    public string AppendEventQuickWithVersionSqlSuffix { get; init; } = string.Empty;

    /// <summary>
    /// Ordered metadata-column binders for the per-event
    /// <c>QuickWithVersion</c> path on this Rich descriptor. Identical to
    /// <see cref="MetadataBinders"/> except the sequence binder is omitted —
    /// <c>seq_id</c> is server-set via the <c>nextval(...)</c> literal in
    /// <see cref="AppendEventQuickWithVersionSqlSuffix"/>.
    /// </summary>
    public IEventMetadataBinder[] MetadataBindersWithoutSequence { get; init; }
        = System.Array.Empty<IEventMetadataBinder>();

    /// <summary>
    /// Whether the events table is conjoined-tenant — every per-stream query
    /// (StreamState lookup, UpdateStreamVersion, etc.) needs a trailing
    /// <c>and tenant_id = $N</c> when this is true.
    /// </summary>
    /// <remarks>
    /// Init-only so the dialect sets it once at descriptor construction. The
    /// closed-shape path lifts the tenancy check to a per-descriptor boolean
    /// so the storage methods don't carry an event-graph reference.
    /// </remarks>
    public bool IsTenancyConjoined { get; init; }

    /// <summary>
    /// The <c>select version from {schema}.mt_streams where id = </c> prefix for the
    /// AssertStreamVersion (AlwaysEnforceConsistency, zero-events) path. Built once by the dialect.
    /// </summary>
    public string AssertStreamVersionSql { get; init; } = string.Empty;

    /// <summary>
    /// Whether streams are identified by <see cref="System.Guid"/> (true) or
    /// <see cref="string"/> (false). The Rich AppendEvent operation reads
    /// <c>Stream.Id</c> vs <c>Stream.Key</c> based on this flag.
    /// </summary>
    public bool IsGuidStreamIdentity { get; init; }

    /// <summary>
    /// The storage dialect used to set provider parameter types on the
    /// per-event append operation's bound parameters
    /// (<see cref="IStorageDialect.SetParameterType"/>), keeping the
    /// closed-shape ops free of any direct provider reference. Installed by
    /// the event dialect at descriptor-build time.
    /// </summary>
    public IStorageDialect Dialect { get; init; } = default!;

    /// <summary>
    /// Configures the <c>mt_streams</c> insert command. The closure owns
    /// the SQL shape (column list, parameter binds, tenancy/identity
    /// variants) and the actual <see cref="IGroupedParameterBuilder"/>
    /// dispatch.
    /// </summary>
    /// <remarks>
    /// Init-only so the dialect installs it at descriptor-build time. Throws
    /// if invoked before a closure is installed (means the dialect hasn't
    /// wired this codepath yet — e.g., a strict-identity-enforcement variant
    /// isn't supported yet).
    /// </remarks>
    public System.Action<ICommandBuilder, StreamAction> ConfigureInsertStreamCommand { get; init; }
        = static (_, _) => throw new System.NotSupportedException(
            "RichEventStorageDescriptor.ConfigureInsertStreamCommand was not installed by the dialect. " +
            "This indicates a Rich-mode configuration variant (e.g., strict stream-identity enforcement) " +
            "that the closed-shape hierarchy doesn't yet cover.");

    /// <summary>
    /// Configures the <c>mt_streams</c> update-version command. Symmetric
    /// to <see cref="ConfigureInsertStreamCommand"/> — SQL shape and binds
    /// owned by the dialect-installed closure.
    /// </summary>
    public System.Action<ICommandBuilder, StreamAction> ConfigureUpdateStreamVersionCommand { get; init; }
        = static (_, _) => throw new System.NotSupportedException(
            "RichEventStorageDescriptor.ConfigureUpdateStreamVersionCommand was not installed by the dialect.");

    /// <summary>
    /// Optional dialect-installed transform that maps a provider exception raised
    /// by the <c>mt_streams</c> insert (e.g. a unique-constraint violation on the
    /// streams table) into a store-specific "stream id already exists" exception,
    /// given the offending <see cref="StreamAction"/>. Returns <c>null</c> to
    /// leave the original exception unchanged. Keeps provider exception types out
    /// of the neutral <see cref="InsertStreamOperationBase"/>: the Postgres
    /// dialect installs the <c>ExistingStreamIdCollisionException</c> mapping, a
    /// SQL Server dialect installs its own. Default: no transform.
    /// </summary>
    public System.Func<System.Exception, StreamAction, System.Exception?>? TransformInsertStreamException { get; init; }
}
