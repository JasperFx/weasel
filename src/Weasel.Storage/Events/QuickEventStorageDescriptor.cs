#nullable enable
using System;
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// Per-event-store-configuration descriptor for the Quick (batch) append
/// flow. Holds only the SQL strings the Quick operations need — no Rich
/// per-row INSERT prefix, no <see cref="IEventMetadataBinder"/> array for
/// the batched call.
/// </summary>
/// <remarks>
/// <para>
/// Why no metadata binder array on the batched call: Quick-mode binds
/// metadata as <i>per-batch arrays</i> — one array parameter per column,
/// with the array's contents being one value per event in the stream. That
/// shape is uniform enough across columns that inlining the bind code per
/// active column is just as clean as a binder dispatch — and avoids the
/// per-event-list allocation churn a per-binder approach would force.
/// </para>
/// <para>
/// The per-event <c>QuickWithVersion</c> INSERT path (new streams and
/// expected-version-on-server appends) DOES use a binder array, exposed via
/// <see cref="MetadataBinders"/> / <see cref="AppendEventFullMetadataBinders"/>.
/// </para>
/// </remarks>
public sealed class QuickEventStorageDescriptor
{
    public QuickEventStorageDescriptor(
        string quickAppendEventsSql,
        string insertStreamSql,
        string updateStreamVersionSql,
        Func<IEvent, string> serializeEventData,
        Func<IEvent, byte[]?> serializeEventBdata)
    {
        QuickAppendEventsSql = quickAppendEventsSql;
        InsertStreamSql = insertStreamSql;
        UpdateStreamVersionSql = updateStreamVersionSql;
        SerializeEventData = serializeEventData;
        SerializeEventBdata = serializeEventBdata;
    }

    /// <summary>
    /// Complete SQL for <c>select {schema}.mt_quick_append_events(</c> —
    /// the operation appends parameters via <see cref="IGroupedParameterBuilder"/>
    /// and the trailing <c>)</c> in <c>ConfigureCommand</c>. The function
    /// signature varies by configuration; the descriptor flags below tell
    /// the operation which columns participate.
    /// </summary>
    public string QuickAppendEventsSql { get; }

    public string InsertStreamSql { get; }
    public string UpdateStreamVersionSql { get; }
    public Func<IEvent, string> SerializeEventData { get; }

    /// <summary>
    ///     Serializer for the <c>bdata</c> bytea column on the per-event
    ///     QuickWithVersion INSERT shape. In Quick modes, binary event types
    ///     are rejected at descriptor-build time, so the dialect installs a
    ///     closure that always returns <c>null</c> — <c>bdata</c> binds as
    ///     DBNull. The slot still exists because <c>mt_events.bdata</c> is
    ///     part of the column list on every full-shape INSERT.
    /// </summary>
    public Func<IEvent, byte[]?> SerializeEventBdata { get; }

    /// <summary>Guid stream identity (writeId) vs string identity (writeKey).</summary>
    public bool IsGuidStreamIdentity { get; init; }

    /// <summary>
    /// The storage dialect used to set provider parameter types on the
    /// per-event QuickWithVersion INSERT's bound parameters
    /// (<see cref="IStorageDialect.SetParameterType"/>), keeping the
    /// closed-shape ops free of any direct provider reference. Installed by
    /// the event dialect at descriptor-build time.
    /// </summary>
    public IStorageDialect Dialect { get; init; } = default!;

    /// <summary>Conjoined-tenant — affects per-stream ops (InsertStream / UpdateStreamVersion / StreamState).</summary>
    public bool IsTenancyConjoined { get; init; }

    /// <summary>
    /// The <c>select version from {schema}.mt_streams where id = </c> prefix for the
    /// AssertStreamVersion (AlwaysEnforceConsistency, zero-events) path. Built once by the dialect.
    /// </summary>
    public string AssertStreamVersionSql { get; init; } = string.Empty;

    /// <summary>Whether the events table has the <c>causation_id</c> column.</summary>
    public bool HasCausationId { get; init; }

    /// <summary>Whether the events table has the <c>correlation_id</c> column.</summary>
    public bool HasCorrelationId { get; init; }

    /// <summary>Whether the events table has the <c>headers</c> jsonb column.</summary>
    public bool HasHeaders { get; init; }

    /// <summary>Whether the events table has the <c>user_name</c> column.</summary>
    public bool HasUserName { get; init; }

    /// <summary>
    /// Whether DCB tag types are configured AND the storage mode wires them
    /// as per-batch <c>varchar[]</c> parameters on <c>mt_quick_append_events</c>.
    /// </summary>
    public bool HasTagWrites { get; init; }

    /// <summary>
    /// <c>UseTenantPartitionedEvents</c> is on, which means the bulk function
    /// carries the trailing <c>expected_version</c> parameter (default NULL)
    /// for the optimistic-concurrency append shapes. The Quick operation
    /// binds the parameter when this flag is on.
    /// </summary>
    public bool UseTenantPartitionedEvents { get; init; }

    /// <summary>
    /// <c>EnableBigIntEvents</c> picks <c>bigint</c> over <c>int</c> for sequence
    /// + version columns, which propagates to the optimistic-concurrency
    /// parameter type — has to match the function-signature width so the
    /// database doesn't refuse the bind on the strict typed path.
    /// </summary>
    public bool UseBigIntEvents { get; init; }

    /// <summary>
    /// SQL prefix <c>insert into mt_events (cols) values (</c> for the
    /// per-event QuickWithVersion path. The Quick appender uses this shape
    /// (one INSERT per event) for new streams and for existing streams
    /// with an expected-version-on-server guard.
    /// </summary>
    public string AppendEventSqlPrefix { get; init; } = string.Empty;

    /// <summary>
    /// SQL suffix for the per-event QuickWithVersion path. Includes the
    /// trailing <c>, nextval('schema.mt_events_sequence'))</c> — server-set
    /// sequence (no client param) and closing paren.
    /// </summary>
    public string AppendEventSqlSuffix { get; init; } = ")";

    /// <summary>
    /// Ordered optional-metadata-column binders for the per-event
    /// QuickWithVersion path. Same shape as
    /// <see cref="RichEventStorageDescriptor.MetadataBinders"/> but WITHOUT
    /// the sequence binder — seq_id is server-set via the nextval-literal in
    /// <see cref="AppendEventSqlSuffix"/>, not a bound parameter.
    /// </summary>
    public IEventMetadataBinder[] MetadataBinders { get; init; } = System.Array.Empty<IEventMetadataBinder>();

    /// <summary>
    /// SQL suffix for the Full-mode per-event INSERT used by
    /// <c>QuickEventStorage.AppendEvent</c> (called by paths like the
    /// tombstone batch regardless of append mode). Just <c>")"</c> — no
    /// <c>nextval()</c> fragment; seq_id is a bound parameter.
    /// </summary>
    public string AppendEventFullSqlSuffix { get; init; } = ")";

    /// <summary>
    /// Metadata binders for the Full-mode per-event INSERT path. Mirror of
    /// <see cref="RichEventStorageDescriptor.MetadataBinders"/> — includes
    /// the sequence binder because seq_id is a bound parameter here (not
    /// <c>nextval()</c>).
    /// </summary>
    public IEventMetadataBinder[] AppendEventFullMetadataBinders { get; init; } = System.Array.Empty<IEventMetadataBinder>();

    /// <summary>
    /// Configures the <c>mt_streams</c> insert command — identical shape to
    /// the Rich descriptor's closure. Init-only; the dialect installs it.
    /// </summary>
    public System.Action<ICommandBuilder, StreamAction> ConfigureInsertStreamCommand { get; init; }
        = static (_, _) => throw new System.NotSupportedException(
            "QuickEventStorageDescriptor.ConfigureInsertStreamCommand was not installed by the dialect.");

    /// <summary>
    /// Configures the <c>mt_streams</c> update-version command — identical
    /// shape to the Rich descriptor's closure.
    /// </summary>
    public System.Action<ICommandBuilder, StreamAction> ConfigureUpdateStreamVersionCommand { get; init; }
        = static (_, _) => throw new System.NotSupportedException(
            "QuickEventStorageDescriptor.ConfigureUpdateStreamVersionCommand was not installed by the dialect.");

    /// <summary>
    /// Creates the batched append operation for a stream. Dialect-installed —
    /// the batch shape is inherently dialect-specific (Postgres binds
    /// per-column array parameters to <c>mt_quick_append_events</c>; another
    /// dialect may use a multi-row INSERT with an OUTPUT/RETURNING read-back),
    /// so the operation type itself lives with the dialect rather than the
    /// shared storage hierarchy. Takes the descriptor as an argument because
    /// init-only slots can't close over the instance under construction.
    /// </summary>
    public System.Func<QuickEventStorageDescriptor, StreamAction, IStorageOperation>
        CreateQuickAppendEventsOperation { get; init; }
        = static (_, _) => throw new System.NotSupportedException(
            "QuickEventStorageDescriptor.CreateQuickAppendEventsOperation was not installed by the dialect.");
}
