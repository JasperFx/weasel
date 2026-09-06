#nullable enable
using JasperFx.Events;
using JasperFx.Events.Projections;

namespace Weasel.Storage;

/// <summary>
/// The set of event types an async daemon shard is allowed to read, in both of the spellings a
/// store's events table can carry: the short type alias (Marten's and Fisher's <c>type</c> column)
/// and the assembly-qualified .NET type name (Polecat's <c>dotnet_type</c> column). Both are
/// supplied so the dialect can filter on whichever column it actually indexes, rather than the base
/// class picking one and forcing two of the three stores to translate.
/// </summary>
/// <remarks>
/// <para>
/// <b>This exists to be rendered into SQL, never applied after hydration.</b> Both Polecat and
/// Fisher moved their event-type filter from a client-side <c>HashSet</c> check into the query
/// (polecat, fisher#153) for the obvious reason: a subscription that names three event types on a
/// store holding thirty should not read, deserialize and discard the other twenty-seven. A dialect
/// that ignored <see cref="IsEmpty"/> and let <see cref="EventLoaderBase"/> filter afterwards would
/// silently undo that, and the only symptom would be throughput.
/// </para>
/// <para>
/// An empty allow list means "every event type", which is the daemon's default — an
/// <see cref="EventFilterable"/> with no <see cref="EventFilterable.IncludedEventTypes"/> is asking
/// for everything, not for nothing.
/// </para>
/// </remarks>
public sealed class EventTypeAllowList
{
    /// <summary>An allow list that admits every event type.</summary>
    public static EventTypeAllowList All { get; } = new([], []);

    private EventTypeAllowList(IReadOnlyList<string> aliases, IReadOnlyList<string> dotNetTypeNames)
    {
        Aliases = aliases;
        DotNetTypeNames = dotNetTypeNames;
    }

    /// <summary>
    /// The stores' short type aliases (e.g. <c>trip_started</c>), matching the <c>type</c> column.
    /// Prefer these where the column is indexed: an alias survives a CLR type being renamed or
    /// moved, where the assembly-qualified name does not.
    /// </summary>
    public IReadOnlyList<string> Aliases { get; }

    /// <summary>
    /// The assembly-qualified .NET type names, matching a <c>dotnet_type</c> column.
    /// </summary>
    public IReadOnlyList<string> DotNetTypeNames { get; }

    /// <summary>True when no filtering applies and every event type should be read.</summary>
    public bool IsEmpty => Aliases.Count == 0;

    /// <summary>
    /// Build the allow list for a projection or subscription's declared event types. Returns
    /// <see cref="All"/> when the filterable names none, which is what an unfiltered shard wants.
    /// </summary>
    /// <remarks>
    /// Takes the concrete <see cref="EventFilterable"/> rather than <see cref="IEventFilterable"/>
    /// deliberately: the interface declares the <c>IncludeType</c> writers and
    /// <see cref="IEventFilterable.IncludeArchivedEvents"/>, but the
    /// <see cref="EventFilterable.IncludedEventTypes"/> collection this reads lives only on the
    /// class. That is also the type all three stores' loaders already accept.
    /// </remarks>
    public static EventTypeAllowList For(IEventRegistry registry, EventFilterable? filtering)
    {
        ArgumentNullException.ThrowIfNull(registry);

        if (filtering?.IncludedEventTypes is not { Count: > 0 } included)
        {
            return All;
        }

        return For(registry, included);
    }

    /// <summary>
    /// Build the allow list for an explicit set of event types.
    /// </summary>
    public static EventTypeAllowList For(IEventRegistry registry, IReadOnlyList<Type> eventTypes)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(eventTypes);

        if (eventTypes.Count == 0)
        {
            return All;
        }

        // Ordinal-distinct rather than a plain projection: two registered CLR types can legitimately
        // share an alias through an upcast chain, and a duplicate would render as a redundant term in
        // the IN list.
        var aliases = new List<string>(eventTypes.Count);
        var dotNetTypeNames = new List<string>(eventTypes.Count);

        foreach (var eventType in eventTypes)
        {
            var mapping = registry.EventMappingFor(eventType);

            if (!aliases.Contains(mapping.EventTypeName, StringComparer.Ordinal))
            {
                aliases.Add(mapping.EventTypeName);
            }

            if (!dotNetTypeNames.Contains(mapping.DotNetTypeName, StringComparer.Ordinal))
            {
                dotNetTypeNames.Add(mapping.DotNetTypeName);
            }
        }

        return new EventTypeAllowList(aliases, dotNetTypeNames);
    }
}
