#nullable enable
using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using JasperFx.Events;
using JasperFx.Events.Upcasting;

namespace Weasel.Storage;

/// <summary>
/// The core columns of one events-table row, read off the reader by the store and handed to
/// <see cref="EventRowReaderBase"/> to hydrate. Nothing here is provider-typed: each store converts
/// its own storage representation (Fisher's TEXT Guids and timestamps, Polecat's
/// <c>datetimeoffset</c>) on the way in.
/// </summary>
/// <remarks>
/// Optional metadata columns (correlation, causation, headers, user name) are not here — they vary
/// per event-store configuration and are read by <see cref="EventRowReaderBase.ReadMetadata"/>.
/// </remarks>
public readonly record struct EventRow
{
    /// <summary>The store-wide sequence (<c>seq_id</c>).</summary>
    public long Sequence { get; init; }

    /// <summary>The event's own id.</summary>
    public Guid Id { get; init; }

    /// <summary>The stream id, on a Guid-identified store.</summary>
    public Guid StreamId { get; init; }

    /// <summary>The stream key, on a string-identified store.</summary>
    public string? StreamKey { get; init; }

    /// <summary>The event's version within its stream.</summary>
    public long Version { get; init; }

    /// <summary>The JSON body. For a binary event, whatever placeholder the store writes.</summary>
    public string? Json { get; init; }

    /// <summary>
    /// The binary body, or null for a JSON event. Non-null is what decides that the row is binary —
    /// per row, not per type, so rows written before a type went binary still read as JSON.
    /// </summary>
    public byte[]? BinaryData { get; init; }

    /// <summary>The stored type alias (<c>type</c> column).</summary>
    public string EventTypeName { get; init; }

    /// <summary>The stored assembly-qualified .NET type name, when the store has one.</summary>
    public string? DotNetTypeName { get; init; }

    /// <summary>When the event was appended.</summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>The event's tenant.</summary>
    public string? TenantId { get; init; }

    /// <summary>Whether the event belongs to an archived stream.</summary>
    public bool IsArchived { get; init; }
}

/// <summary>
/// Dialect-neutral hydration of one events-table row into an <see cref="IEvent"/>: upcast first,
/// otherwise resolve the stored .NET type and deserialize, then wrap and stamp the row's metadata.
/// The shape Marten, Polecat and Fisher each carried a copy of (weasel#743).
/// </summary>
/// <remarks>
/// <para>
/// <b>The store reads the columns; this class decides what they mean.</b> A store supplies
/// <see cref="ReadRow"/> (its column order and type conversions), <see cref="CreateUpcastPayload"/>
/// (its serializer behind JasperFx's <see cref="IUpcastPayload"/>) and
/// <see cref="DeserializeData"/> (JSON or binary). Everything with a correctness rule attached —
/// the order of the upcast and type-resolution checks, which exceptions mean what to the daemon —
/// lives here once.
/// </para>
/// <para>
/// <b>Upcast first.</b> A registered transformation is consulted before the stored
/// <c>dotnet_type</c>, so the type hint cannot shadow it (marten#4680): a legacy row whose old CLR
/// type still resolves would otherwise hydrate as the old type and never reach the upcaster.
/// </para>
/// <para>
/// <b>An unresolvable type returns null; it does not throw and it does not skip.</b> The caller
/// owns that policy — <see cref="EventLoaderBase"/> applies the shard's
/// <c>SkipUnknownEvents</c>, while a stream read may skip unconditionally. A body that cannot be
/// read (including an upcast that throws) raises <see cref="EventDeserializationFailureException"/>,
/// which declares <c>ShardFailureCategory.EventSerialization</c> through
/// <c>IEventFailureContext</c>, so the loader's <c>SkipSerializationErrors</c> governs it without
/// knowing anything about this class.
/// </para>
/// <para>
/// <b>It never filters.</b> The event-type allow list is SQL (see <see cref="EventTypeAllowList"/>);
/// re-checking a hydrated event against it here would be a second, divergent filter.
/// </para>
/// <para>
/// An <see cref="EventLoaderBase"/> delegates to it in one line:
/// <c>protected override ValueTask&lt;IEvent?&gt; ReadEventAsync(DbDataReader r, CancellationToken t) =&gt; _rows.ReadAsync(r, t);</c>
/// </para>
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Hydrates events through the store's registered event mappings; event types are rooted by their registration, as in each store's own loader.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "IEventType.Wrap closes Event<T> over a registered event type; AOT consumers register their event types ahead of time.")]
public abstract class EventRowReaderBase
{
    private readonly ConcurrentDictionary<string, Type?> _typesByDotNetName = new(StringComparer.Ordinal);

    /// <param name="registry">The store's event registry: mappings and upcasters.</param>
    protected EventRowReaderBase(EventRegistry registry)
    {
        Registry = registry ?? throw new ArgumentNullException(nameof(registry));
    }

    /// <summary>The store's event registry.</summary>
    public EventRegistry Registry { get; }

    /// <summary>
    /// Hydrate the reader's current row, running an upcast through its asynchronous delegate.
    /// Returns null when the row's stored .NET type resolves to nothing in this deployment.
    /// </summary>
    /// <remarks>
    /// Every awaited read — the daemon's above all — must come through here rather than
    /// <see cref="Read"/>: an async-only upcaster is unreachable through the synchronous delegate,
    /// and the contract makes that loud rather than silently falling back.
    /// </remarks>
    public async ValueTask<IEvent?> ReadAsync(DbDataReader reader, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var row = ReadRow(reader);

        object data;
        IEventType mapping;

        if (TryFindUpcast(row, out var transformation))
        {
            try
            {
                data = await transformation.UpcastAsync(CreateUpcastPayload(row), token).ConfigureAwait(false);
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                throw DeserializationFailure(row, e);
            }

            mapping = Registry.EventMappingFor(transformation.EventType);
        }
        else
        {
            var eventType = ResolveEventType(row.DotNetTypeName);
            if (eventType is null)
            {
                return null;
            }

            try
            {
                data = await DeserializeDataAsync(eventType, row, token).ConfigureAwait(false);
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                throw DeserializationFailure(row, e);
            }

            mapping = Registry.EventMappingFor(eventType);
        }

        return Finish(reader, row, mapping, data);
    }

    /// <summary>
    /// Hydrate the reader's current row on a synchronous read path. Returns null when the row's
    /// stored .NET type resolves to nothing in this deployment.
    /// </summary>
    public IEvent? Read(DbDataReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var row = ReadRow(reader);

        object data;
        IEventType mapping;

        if (TryFindUpcast(row, out var transformation))
        {
            try
            {
                data = transformation.Upcast(CreateUpcastPayload(row));
            }
            catch (Exception e)
            {
                throw DeserializationFailure(row, e);
            }

            mapping = Registry.EventMappingFor(transformation.EventType);
        }
        else
        {
            var eventType = ResolveEventType(row.DotNetTypeName);
            if (eventType is null)
            {
                return null;
            }

            try
            {
                data = DeserializeData(eventType, row);
            }
            catch (Exception e)
            {
                throw DeserializationFailure(row, e);
            }

            mapping = Registry.EventMappingFor(eventType);
        }

        return Finish(reader, row, mapping, data);
    }

    /// <summary>Read the core columns of the reader's current row.</summary>
    protected abstract EventRow ReadRow(DbDataReader reader);

    /// <summary>The store's upcast payload over the row's stored JSON.</summary>
    protected abstract IUpcastPayload CreateUpcastPayload(EventRow row);

    /// <summary>
    /// Deserialize the row's body as <paramref name="eventType"/> — from
    /// <see cref="EventRow.BinaryData"/> when it is non-null, otherwise from <see cref="EventRow.Json"/>.
    /// </summary>
    protected abstract object DeserializeData(Type eventType, EventRow row);

    /// <summary>
    /// The asynchronous form of <see cref="DeserializeData"/>. Defaults to the synchronous one;
    /// override where the serializer has a genuinely asynchronous path.
    /// </summary>
    protected virtual ValueTask<object> DeserializeDataAsync(Type eventType, EventRow row, CancellationToken token)
        => new(DeserializeData(eventType, row));

    /// <summary>
    /// Read the optional metadata columns (correlation, causation, headers, user name) onto the
    /// hydrated event. The default reads none.
    /// </summary>
    protected virtual void ReadMetadata(DbDataReader reader, IEvent @event)
    {
    }

    /// <summary>
    /// The registered transformation for this row, if any. Only a JSON row is upcast: a binary body
    /// has no JSON for a transformation to read.
    /// </summary>
    protected virtual bool TryFindUpcast(EventRow row, [NotNullWhen(true)] out UpcastTransformation? transformation)
    {
        if (row.BinaryData is not null || row.EventTypeName is null || !Registry.Upcasters.HasAny)
        {
            transformation = null;
            return false;
        }

        return Registry.Upcasters.TryFindTransformation(row.EventTypeName, out transformation);
    }

    /// <summary>
    /// Resolve a stored assembly-qualified .NET type name, or null when this process does not know
    /// it. Memoized, misses included, so a foreign type stored by another application does not
    /// re-probe every assembly for every row it appears on.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2057:UnrecognizedReflectionPattern",
        Justification = "Resolves stored event type names; event types are rooted by their registration.")]
    protected virtual Type? ResolveEventType(string? dotNetTypeName)
    {
        if (string.IsNullOrEmpty(dotNetTypeName))
        {
            return null;
        }

        return _typesByDotNetName.GetOrAdd(dotNetTypeName, static name => Type.GetType(name));
    }

    /// <summary>
    /// The exception to raise when a row's body cannot be read. Must declare
    /// <c>ShardFailureCategory.EventSerialization</c> through <c>IEventFailureContext</c>; override
    /// to return the store's own subclass.
    /// </summary>
    /// <remarks>
    /// Reports the stored type alias rather than the assembly-qualified name, matching what the
    /// daemon's shard failure carries everywhere else.
    /// </remarks>
    protected virtual Exception DeserializationFailure(EventRow row, Exception inner)
        => new EventDeserializationFailureException(row.Sequence, row.EventTypeName, inner);

    private IEvent Finish(DbDataReader reader, EventRow row, IEventType mapping, object data)
    {
        var @event = mapping.Wrap(data);

        @event.Id = row.Id;
        @event.Sequence = row.Sequence;
        @event.Version = row.Version;
        @event.Timestamp = row.Timestamp;
        @event.TenantId = row.TenantId!;
        @event.EventTypeName = row.EventTypeName;
        @event.DotNetTypeName = row.DotNetTypeName ?? mapping.DotNetTypeName;
        @event.IsArchived = row.IsArchived;

        if (Registry.StreamIdentity == StreamIdentity.AsGuid)
        {
            @event.StreamId = row.StreamId;
        }
        else
        {
            @event.StreamKey = row.StreamKey;
        }

        ReadMetadata(reader, @event);
        return @event;
    }
}
