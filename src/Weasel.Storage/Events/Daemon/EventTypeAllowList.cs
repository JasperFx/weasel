#nullable enable
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using JasperFx.Events.Upcasting;

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
/// <para>
/// <b>Three kinds of name end up on a non-empty list beyond the declared types</b> (weasel#743), and
/// each one closes a silent hole that all three stores fell into separately:
/// </para>
/// <list type="bullet">
/// <item>
/// <see cref="Archived"/> and <see cref="Compacted{T}"/>, for a single-stream aggregate projection.
/// See <see cref="FrameworkMarkerTypes"/>.
/// </item>
/// <item>
/// <see cref="UpcastSourceAliases"/> — the stored names an upcast transformation reads from. A
/// projection declares the <em>new</em> types; every row written before the migration carries the
/// <em>old</em> name. Filter on the declared set alone and the shard reads none of the history
/// upcasting exists to read, and reports itself caught up (fisher#191, polecat#561).
/// </item>
/// </list>
/// </remarks>
public sealed class EventTypeAllowList
{
    /// <summary>An allow list that admits every event type.</summary>
    public static EventTypeAllowList All { get; } = new([], [], []);

    private EventTypeAllowList(IReadOnlyList<string> aliases, IReadOnlyList<string> dotNetTypeNames,
        IReadOnlyList<string> upcastSourceAliases)
    {
        Aliases = aliases;
        DotNetTypeNames = dotNetTypeNames;
        UpcastSourceAliases = upcastSourceAliases;
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

    /// <summary>
    /// The stored type names (<c>type</c> column) of rows a registered upcast transformation turns
    /// into one of the allowed types. Empty when no transformation targets an allowed type, which is
    /// the overwhelming majority and leaves a dialect's rendered SQL exactly as it was.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>A separate list, not more entries in <see cref="Aliases"/> and
    /// <see cref="DotNetTypeNames"/></b>, because an upcast source has no CLR type to speak of: a
    /// raw-JSON transformation may name an event type that no longer exists in any assembly, so there
    /// is no <c>dotnet_type</c> value to add. A dialect filtering on <c>type</c> can match both lists
    /// in one <c>IN</c> (see <see cref="AliasesIncludingUpcastSources"/>); a dialect filtering on
    /// <c>dotnet_type</c> has to render a second disjunct, <c>or type in (...)</c>, for these.
    /// </para>
    /// <para>
    /// Only transformations whose <em>target</em> is an allowed type contribute (fisher#191's rule).
    /// Admitting every source in the registry would hand a filtered shard rows that upcast into types
    /// it then has no use for.
    /// </para>
    /// </remarks>
    public IReadOnlyList<string> UpcastSourceAliases { get; }

    /// <summary>
    /// <see cref="Aliases"/> followed by any <see cref="UpcastSourceAliases"/> not already among them
    /// — everything a dialect matching on the <c>type</c> column should put in its <c>IN</c> list.
    /// </summary>
    public IReadOnlyList<string> AliasesIncludingUpcastSources => UpcastSourceAliases.Count == 0
        ? Aliases
        : [.. Aliases, .. UpcastSourceAliases.Where(x => !Aliases.Contains(x, StringComparer.Ordinal))];

    /// <summary>True when no filtering applies and every event type should be read.</summary>
    /// <remarks>
    /// Decided by the declared types alone. Upcast sources only ever widen a list that is already
    /// filtering; they never turn an unfiltered shard into a filtered one.
    /// </remarks>
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

        var eventTypes = new List<Type>(included);
        foreach (var marker in FrameworkMarkerTypes(filtering))
        {
            if (!eventTypes.Contains(marker))
            {
                eventTypes.Add(marker);
            }
        }

        return For(registry, eventTypes);
    }

    /// <summary>
    /// The event types the <em>framework</em> writes into a stream, which no projection declares an
    /// <c>Apply</c> or <c>Create</c> for and which are therefore not in
    /// <see cref="EventFilterable.IncludedEventTypes"/> on their own account.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="Compacted{T}"/> is the one that bites. Compacting a stream folds its history into
    /// a snapshot, <b>deletes</b> the folded events and leaves the marker in their place — the
    /// marker is not an event beside the history, it <em>is</em> the history. Filter it out and the
    /// events it replaced are not filtered too, they are gone: the shard folds only what was appended
    /// after compaction and reports an aggregate missing everything before it, with no error
    /// anywhere.
    /// </para>
    /// <para>
    /// <see cref="Archived"/> is what makes an async single-stream shard archive the stream; a shard
    /// that never sees it never does.
    /// </para>
    /// <para>
    /// <b>Redundant on current JasperFx, deliberately.</b> jasperfx#784 and jasperfx#796 put both
    /// markers into a single-stream projection's own <c>determineEventTypes()</c>, so they normally
    /// arrive in <c>IncludedEventTypes</c> already and adding them here is a no-op. It stays as the
    /// belt to that fix's braces (polecat#568): the cost is two list checks per loader, and the
    /// failure it guards is a confidently wrong aggregate with nothing logged. It also covers an
    /// <see cref="IAggregateProjection"/> that does not derive from JasperFx's single-stream base,
    /// which the upstream rule cannot reach. Do not delete it for being redundant.
    /// </para>
    /// <para>
    /// Scoped to a <b>single-stream</b> aggregate projection only. Both markers are facts about one
    /// stream's own history; a subscription, an event projection or a multi-stream projection has no
    /// aggregate to close <see cref="Compacted{T}"/> over and nothing to do with either. Deliberately
    /// absent: <c>Tombstone</c> (a sequence-gap filler no projection folds).
    /// </para>
    /// </remarks>
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Guarded by RuntimeFeature.IsDynamicCodeSupported for value-type aggregates; a reference-type argument closes over the shared canonical instantiation.")]
    [UnconditionalSuppressMessage("Trimming", "IL2055:MakeGenericType",
        Justification = "Compacted<T> has no constraints and its type argument is the projection's registered aggregate type.")]
    public static IEnumerable<Type> FrameworkMarkerTypes(EventFilterable? filtering)
    {
        if (filtering is not IAggregateProjection { Scope: AggregationScope.SingleStream } aggregateProjection)
        {
            yield break;
        }

        yield return typeof(Archived);

        var aggregateType = aggregateProjection.AggregateType;

        // A value-type argument needs code ILC never generated (see CLAUDE.md, "A Type-keyed seam
        // cannot be a generic"). Under Native AOT such a projection relies on the upstream
        // determineEventTypes() having already declared Compacted<T> for it.
        if (!aggregateType.IsValueType || RuntimeFeature.IsDynamicCodeSupported)
        {
            yield return typeof(Compacted<>).MakeGenericType(aggregateType);
        }
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

        return new EventTypeAllowList(aliases, dotNetTypeNames,
            UpcastSourcesFor((registry as EventRegistry)?.Upcasters, eventTypes, aliases));
    }

    private static IReadOnlyList<string> UpcastSourcesFor(UpcastingRegistry? upcasters,
        IReadOnlyList<Type> eventTypes, List<string> aliases)
    {
        if (upcasters is not { HasAny: true })
        {
            return [];
        }

        List<string>? sources = null;

        foreach (var transformation in upcasters.AllTransformations)
        {
            if (!eventTypes.Contains(transformation.EventType))
            {
                continue;
            }

            // Ordinal-distinct, and never a name the declared list already carries: a source that is
            // also a live alias needs no second term in the rendered SQL.
            if (aliases.Contains(transformation.EventTypeName, StringComparer.Ordinal))
            {
                continue;
            }

            sources ??= [];
            if (!sources.Contains(transformation.EventTypeName, StringComparer.Ordinal))
            {
                sources.Add(transformation.EventTypeName);
            }
        }

        return sources is null ? [] : sources;
    }
}
