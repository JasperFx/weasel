#nullable enable
namespace Weasel.Storage;

/// <summary>
/// One bounded read of the events table, described without any SQL in it. This is the whole of what
/// <see cref="EventLoaderBase"/> asks an <see cref="IEventPagingDialect"/> for, and it is the same
/// question in all three Critter Stack stores: the events in <c>(Floor, Ceiling]</c>, in sequence
/// order, at most <see cref="BatchSize"/> of them, excluding archived events unless asked for, and
/// restricted to one tenant and/or one set of event types when the shard is scoped that way.
/// </summary>
/// <remarks>
/// <para>
/// <b><see cref="Floor"/> is exclusive and <see cref="Ceiling"/> is inclusive</b> — the daemon's
/// progression is "the last sequence I have processed", so the next page starts strictly above it.
/// Every store already spells this <c>seq_id &gt; @floor and seq_id &lt;= @ceiling</c>; a dialect
/// that rendered it any other way would double-process or skip an event at each page boundary.
/// </para>
/// <para>
/// <see cref="Ceiling"/> is not necessarily the shard's high-water mark. A store layering a
/// window-stepping strategy over the base (see <see cref="EventLoaderBase.LoadByWindowAsync"/>)
/// asks for a much narrower range, and the page it gets back reports its ceiling against the window
/// it actually scanned rather than against the high-water mark it did not.
/// </para>
/// </remarks>
public sealed record EventPageQuery
{
    /// <summary>Exclusive lower bound on <c>seq_id</c>.</summary>
    public required long Floor { get; init; }

    /// <summary>Inclusive upper bound on <c>seq_id</c>.</summary>
    public required long Ceiling { get; init; }

    /// <summary>
    /// Maximum rows to return. Rendered as <c>TOP(@n)</c>, <c>LIMIT @n</c> or <c>FETCH FIRST</c>
    /// depending on the dialect — this is one of only two things that genuinely differ between the
    /// three stores' loaders, the other being row hydration.
    /// </summary>
    public required int BatchSize { get; init; }

    /// <summary>
    /// When set, restrict the read to one tenant. Non-null only on a per-tenant rebuild or
    /// subscription shard (Marten's #4596, Polecat's #163); null means store-global.
    /// </summary>
    public string? TenantId { get; init; }

    /// <summary>
    /// The event types this shard reads. <b>The dialect must render this into the WHERE clause</b>
    /// — see <see cref="EventTypeAllowList"/> for why filtering after hydration is not an
    /// acceptable substitute.
    /// </summary>
    public EventTypeAllowList EventTypes { get; init; } = EventTypeAllowList.All;

    /// <summary>
    /// False (the default) excludes archived events, which is what every ordinary shard wants and
    /// what each store's <c>(is_archived, seq_id)</c> index is shaped for. True is the deliberate
    /// "replay everything including archived streams" case.
    /// </summary>
    public bool IncludeArchivedEvents { get; init; }
}
