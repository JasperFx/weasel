#nullable enable
namespace Weasel.Storage;

/// <summary>
/// What one daemon shard's loader is scoped to read. Fixed when the store builds the loader, and
/// folded into every <see cref="EventPageQuery"/> the loader issues.
/// </summary>
public sealed class EventLoaderOptions
{
    /// <summary>
    /// Restrict the load to one tenant. Set on a per-tenant rebuild or subscription shard so it
    /// reads only that tenant's bounded sequence range rather than the whole store; null (the
    /// default) is store-global.
    /// </summary>
    /// <remarks>
    /// Worth setting even where the store is not partitioned by tenant. On a per-tenant rebuild it
    /// is not only an optimisation: without it the execution sees other tenants' events and routes
    /// their document writes by <c>IEvent.TenantId</c>, so a rebuild for one tenant writes another
    /// tenant's documents.
    /// </remarks>
    public string? TenantId { get; init; }

    /// <summary>
    /// The event types this shard reads, rendered into the query's WHERE clause by the dialect.
    /// Defaults to <see cref="EventTypeAllowList.All"/>.
    /// </summary>
    public EventTypeAllowList EventTypes { get; init; } = EventTypeAllowList.All;

    /// <summary>
    /// Whether archived events are in scope. False by default, which is what an ordinary shard
    /// wants and what each store's <c>(is_archived, seq_id)</c> index is shaped for.
    /// </summary>
    public bool IncludeArchivedEvents { get; init; }
}
