#nullable enable
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;

namespace Weasel.Storage;

/// <summary>
/// The dialect-neutral inner event loader for the async daemon. Pages an events table by sequence,
/// hydrates each row, honours the shard's skip policy and reports an honest ceiling — the shape
/// Marten, Polecat and Fisher each carried a copy of (weasel#566).
/// </summary>
/// <remarks>
/// <para>
/// <b>This is the bare inner loader.</b> Retry and load metrics are layered on by JasperFx's
/// <c>ResilientEventLoader</c> decorator when a store builds its loader, so nothing here handles
/// either.
/// </para>
/// <para>
/// <b>The base is a set of primitives, not a single algorithm.</b> Marten's loader escalates
/// through three strategies when the ordinary query times out — normal, then skip-ahead, then
/// window-step — and Polecat and Fisher have nothing of the kind. A base that only offered "load
/// one page" would have left Marten unable to adopt it, which is the store the duplication costs
/// the most. So <see cref="LoadAsync"/> is virtual and does the simple thing, and the pieces the
/// progression is built from are protected and composable:
/// </para>
/// <list type="bullet">
/// <item><see cref="LoadRangeAsync(EventRequest,long,long,CancellationToken)"/> — one bounded page.</item>
/// <item><see cref="ProbeNextMatchingSequenceAsync"/> — the skip-ahead probe.</item>
/// <item><see cref="LoadByWindowAsync"/> — the window-step walk, written over the other two.</item>
/// </list>
/// <para>
/// A store adds escalation by overriding <see cref="LoadAsync"/> and choosing between them; it
/// never has to reimplement paging, skip accounting or the ceiling calculation underneath.
/// </para>
/// <para>
/// <b>The event-type filter is SQL.</b> See <see cref="EventTypeAllowList"/>: the allow list is
/// handed to the dialect and rendered into the WHERE clause. This class never filters a hydrated
/// event, because doing so would quietly undo the work polecat and fisher#153 did.
/// </para>
/// </remarks>
public abstract class EventLoaderBase: IEventLoader
{
    /// <summary>
    /// The window a <see cref="LoadByWindowAsync"/> walk advances by when the caller does not name
    /// one. Marten's value, and the reasoning is the same: small enough that a window cannot time
    /// out on a store where the ordinary query already did.
    /// </summary>
    public const long DefaultWindowSize = 10_000;

    /// <param name="dialect">Renders this store's page and probe SQL.</param>
    /// <param name="options">The shard's scope — tenant, event types, archived events.</param>
    protected EventLoaderBase(IEventPagingDialect dialect, EventLoaderOptions? options = null)
    {
        Dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
        Options = options ?? new EventLoaderOptions();
    }

    /// <summary>The SQL seam.</summary>
    public IEventPagingDialect Dialect { get; }

    /// <summary>What this shard is scoped to read.</summary>
    public EventLoaderOptions Options { get; }

    /// <summary>
    /// Load one page for the request. The default implementation reads the whole
    /// <c>(Floor, HighWater]</c> range in one query; override to layer a strategy progression over
    /// the primitives below.
    /// </summary>
    public virtual async Task<EventPage> LoadAsync(EventRequest request, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);

        try
        {
            return await LoadRangeAsync(request, request.Floor, request.HighWater, token)
                .ConfigureAwait(false);
        }
        catch (Exception e) when (ShouldTranslateToCancellation(e, token))
        {
            throw AsCancellation(e, token);
        }
    }

    /// <summary>
    /// Read one page bounded by <c>(floor, ceiling]</c>. The page reports its floor as
    /// <paramref name="floor"/>.
    /// </summary>
    protected Task<EventPage> LoadRangeAsync(
        EventRequest request, long floor, long ceiling, CancellationToken token)
        => LoadRangeAsync(request, floor, ceiling, floor, token);

    /// <summary>
    /// Read one page bounded by <c>(floor, ceiling]</c>, reporting the page's floor as
    /// <paramref name="pageFloor"/>.
    /// </summary>
    /// <remarks>
    /// The two floors come apart on exactly the strategies that move the query's floor without the
    /// shard having moved: a skip-ahead probe lands the query just below the first matching event
    /// and the page's floor moves with it, because the probe <em>proved</em> there was nothing in
    /// between; a window-step walk keeps the page on the request's original floor, because it has
    /// proved nothing — it is simply reading a narrower slice at a time.
    /// </remarks>
    protected async Task<EventPage> LoadRangeAsync(
        EventRequest request, long floor, long ceiling, long pageFloor, CancellationToken token)
    {
        var (page, _) = await loadRangeAsync(request, floor, ceiling, pageFloor, token).ConfigureAwait(false);
        return page;
    }

    /// <summary>
    /// The lowest sequence above <paramref name="floor"/> and at or below <paramref name="ceiling"/>
    /// matching this shard's filters, or null when there is none.
    /// </summary>
    /// <remarks>
    /// The skip-ahead primitive. Its value is entirely in what it lets a caller <em>not</em> read:
    /// on a store where matching events are sparse, jumping the query's floor to the next match
    /// turns a scan of a large sequence range into a page read.
    /// </remarks>
    protected async Task<long?> ProbeNextMatchingSequenceAsync(
        long floor, long ceiling, int batchSize, CancellationToken token)
    {
        var command = Dialect.BuildNextMatchingSequenceCommand(QueryFor(floor, ceiling, batchSize));

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var cmd = CreateCommand(connection, command);
        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

        // A single-row probe, so "no row" is the no-match answer. Do not test IsDBNull here: that
        // was the shape a min() probe needed, and min() is exactly what the dialect contract says
        // not to render.
        long? sequence = await reader.ReadAsync(token).ConfigureAwait(false)
            ? reader.GetInt64(0)
            : null;

        await reader.CloseAsync().ConfigureAwait(false);
        return sequence;
    }

    /// <summary>
    /// Skip-ahead: probe for the next matching sequence and read a page from just below it. Returns
    /// an empty page at the high-water mark when nothing in range matches.
    /// </summary>
    protected async Task<EventPage> LoadWithSkipAheadAsync(EventRequest request, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);

        var next = await ProbeNextMatchingSequenceAsync(
            request.Floor, request.HighWater, request.BatchSize, token).ConfigureAwait(false);

        if (next is null)
        {
            // Nothing matches anywhere in the range, so the shard really has consumed all of it.
            return EmptyPage(request, request.Floor, request.HighWater);
        }

        // The floor is exclusive, so start one below the match to include it.
        return await LoadRangeAsync(request, next.Value - 1, request.HighWater, token).ConfigureAwait(false);
    }

    /// <summary>
    /// Window-step: advance through the sequence in fixed windows until a window returns rows.
    /// Each window is narrow enough to survive on a store where the full-range query times out.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The page's ceiling is the window's, never the high-water mark</b>, and that is the whole
    /// correctness content of this method (marten#5239). <c>CalculateCeiling</c>'s contract is "if
    /// the page did not fill the batch, the query exhausted everything up to this bound" — and the
    /// query was bounded by the window. Claiming the high-water mark asserts an exhaustion that
    /// never happened, and since the consumer writes the ceiling as durable projection progress,
    /// every matching event above the window is skipped permanently. A window returning fewer than
    /// a full batch is the ordinary case here, not an edge case: the window is wide and this
    /// strategy exists because matching events are sparse.
    /// </para>
    /// <para>
    /// <b>It returns on any row read, not on any event added.</b> A window whose rows were all
    /// skipped may still have had matching events truncated by the batch limit further up the same
    /// window; advancing past it would drop them exactly as the ceiling bug did.
    /// </para>
    /// </remarks>
    protected async Task<EventPage> LoadByWindowAsync(
        EventRequest request, long windowSize, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(windowSize);

        var currentFloor = request.Floor;

        while (currentFloor < request.HighWater)
        {
            var windowCeiling = Math.Min(currentFloor + windowSize, request.HighWater);

            // The page keeps the request's floor: this walk has not proved anything about the range
            // below the current window, it is only reading it in slices.
            var (page, rowsRead) = await loadRangeAsync(
                request, currentFloor, windowCeiling, request.Floor, token).ConfigureAwait(false);

            if (rowsRead > 0)
            {
                return page;
            }

            // No rows at all, so nothing in this window was elided by the batch limit and stepping
            // over it is safe.
            currentFloor = windowCeiling;
        }

        // The walk really did scan every window up to the high-water mark, so claiming it here — as
        // the loop above must not — is honest.
        return EmptyPage(request, request.Floor, request.HighWater);
    }

    /// <summary>
    /// Build the query for one bounded read, folding in this shard's tenant, event-type and
    /// archived-event scope. Override to add a store-specific dimension.
    /// </summary>
    protected virtual EventPageQuery QueryFor(long floor, long ceiling, int batchSize) => new()
    {
        Floor = floor,
        Ceiling = ceiling,
        BatchSize = batchSize,
        TenantId = Options.TenantId,
        EventTypes = Options.EventTypes,
        IncludeArchivedEvents = Options.IncludeArchivedEvents
    };

    /// <summary>Open a connection to read from. The base disposes whatever it is given.</summary>
    protected abstract ValueTask<DbConnection> OpenConnectionAsync(CancellationToken token);

    /// <summary>
    /// Hydrate the reader's current row into an event, or return null when the row's stored .NET
    /// type resolves to nothing in this deployment.
    /// </summary>
    /// <remarks>
    /// Returning null and throwing a <see cref="IEventFailureContext"/> exception with
    /// <see cref="ShardFailureCategory.UnknownEventType"/> mean the same thing to the base; both
    /// spellings exist because the three stores use both. Either way the shard's
    /// <see cref="ErrorHandlingOptions.SkipUnknownEvents"/> decides what happens, because a daemon
    /// that silently skipped an event it could not resolve would leave the projection permanently
    /// wrong with nothing to signal it.
    /// </remarks>
    protected abstract ValueTask<IEvent?> ReadEventAsync(DbDataReader reader, CancellationToken token);

    /// <summary>
    /// Read the sequence of the reader's current row. Defaults to ordinal 0, which is where all
    /// three stores put it; override if the dialect's column list does not.
    /// </summary>
    /// <remarks>
    /// The base reads this for every row, including rows it goes on to skip, because a page whose
    /// rows were all skipped still has to be able to say how far its query actually got.
    /// </remarks>
    protected virtual long ReadSequence(DbDataReader reader) => reader.GetInt64(0);

    /// <summary>
    /// The exception to throw for a row whose <see cref="ReadEventAsync"/> returned null while
    /// <see cref="ErrorHandlingOptions.SkipUnknownEvents"/> is off.
    /// </summary>
    /// <remarks>
    /// Override to return the store's own exception, which should implement
    /// <see cref="IEventFailureContext"/> so the daemon can report
    /// <see cref="ShardFailureCategory.UnknownEventType"/> with the offending sequence rather than
    /// classifying the pause as <see cref="ShardFailureCategory.Other"/> with no detail.
    /// </remarks>
    protected virtual Exception UnresolvedEventTypeException(DbDataReader reader, long sequence)
        => new InvalidOperationException(
            $"Unable to resolve the .NET type of the event at sequence {sequence}. Register the event type, or turn on ErrorHandlingOptions.SkipUnknownEvents to skip it.");

    /// <summary>
    /// Should a hydration failure be skipped rather than thrown, given the shard's error policy?
    /// </summary>
    /// <remarks>
    /// Classification goes through <see cref="IEventFailureContext.Category"/> — the store's own
    /// exception declares its kind, exactly as jasperfx#565 intended, so nothing here sniffs
    /// exception type names.
    /// </remarks>
    protected virtual bool ShouldSkip(Exception ex, ErrorHandlingOptions options) =>
        Classify(ex) switch
        {
            ShardFailureCategory.UnknownEventType => options.SkipUnknownEvents,
            ShardFailureCategory.EventSerialization => options.SkipSerializationErrors,
            _ => false
        };

    /// <summary>
    /// The failure category an exception declares, or null when it declares none.
    /// </summary>
    /// <remarks>
    /// Walks the inner-exception chain, because a store's exception transformer may have wrapped
    /// the one that knows — inspecting only the outermost exception is how marten#4720's timeout
    /// detection missed every wrapped case for as long as it did.
    /// </remarks>
    public static ShardFailureCategory? Classify(Exception? ex)
    {
        while (ex is not null)
        {
            if (ex is IEventFailureContext context)
            {
                return context.Category;
            }

            ex = ex.InnerException;
        }

        return null;
    }

    /// <summary>
    /// Called for each row the loader skipped. The default does nothing; override to record a dead
    /// letter or log, as Marten does for a suppressed serialization failure.
    /// </summary>
    protected virtual ValueTask RecordSkippedEventAsync(
        Exception ex, EventRequest request, CancellationToken token) => default;

    /// <summary>
    /// Should this exception be reported as cooperative cancellation?
    /// </summary>
    /// <remarks>
    /// The daemon cancels a load mid-flight as a matter of course — shutting a shard down after
    /// <c>CatchUpAsync</c> reaches the high-water mark, for one — and an ADO.NET provider does not
    /// necessarily surface that as a clean <see cref="OperationCanceledException"/>. SqlClient
    /// reports it as a <c>SqlException</c> ("Operation cancelled by user"), which the daemon's
    /// recorder would otherwise treat as a real shard error.
    /// </remarks>
    protected virtual bool ShouldTranslateToCancellation(Exception ex, CancellationToken token)
        => token.IsCancellationRequested && ex is not OperationCanceledException;

    /// <summary>The cancellation exception to raise in place of a provider's own.</summary>
    protected virtual Exception AsCancellation(Exception ex, CancellationToken token)
        => new OperationCanceledException("Event loading was cancelled.", ex, token);

    /// <summary>
    /// Tell the page how far its query actually got, whether or not any row survived into it.
    /// </summary>
    /// <param name="page">The page about to have its ceiling calculated.</param>
    /// <param name="lastObservedSequence">
    /// The sequence of the last row the reader saw, or null when the query returned no rows at all.
    /// </param>
    /// <remarks>
    /// <para>
    /// The default does nothing. <c>EventPage.LastObservedSequence</c> (jasperfx#667) is what this
    /// is for — a full batch whose every row was skipped has no last event to take a ceiling from,
    /// and claiming the high-water mark there writes durable progress past rows nobody read. But it
    /// arrived after the JasperFx.Events floor this package builds against, so the base tracks the
    /// value and hands it over here rather than setting it directly.
    /// </para>
    /// <para>
    /// A store on a new enough JasperFx.Events should override this with
    /// <c>page.LastObservedSequence = lastObservedSequence;</c> — one line, and it closes the case
    /// that matters most: a whole event type that stopped resolving, which is exactly what fills a
    /// batch with skips.
    /// </para>
    /// </remarks>
    protected virtual void ReportLastObservedSequence(EventPage page, long? lastObservedSequence)
    {
    }

    /// <summary>
    /// An empty page whose ceiling claims everything up to <paramref name="ceiling"/> was scanned.
    /// </summary>
    protected static EventPage EmptyPage(EventRequest request, long pageFloor, long ceiling)
    {
        var page = new EventPage(pageFloor);
        page.CalculateCeiling(request.BatchSize, ceiling, 0);
        return page;
    }

    [SuppressMessage("Reliability", "CA2100:Review SQL queries for security vulnerabilities",
        Justification = "The SQL is rendered by the store's own dialect from an EventPageQuery whose only free values are bound as parameters.")]
    private DbCommand CreateCommand(DbConnection connection, EventPageCommand command)
    {
        var cmd = connection.CreateCommand();
        cmd.CommandText = command.Sql;

        foreach (var parameter in command.Parameters)
        {
            var dbParameter = cmd.CreateParameter();
            dbParameter.ParameterName = parameter.Name;
            dbParameter.Value = parameter.Value ?? DBNull.Value;

            if (parameter.DbType.HasValue)
            {
                dbParameter.DbType = parameter.DbType.Value;
            }

            cmd.Parameters.Add(dbParameter);
        }

        ConfigureCommand(cmd);
        return cmd;
    }

    /// <summary>
    /// Last look at the command before it executes — a store's command timeout, or enlisting it in
    /// a transaction it already holds.
    /// </summary>
    protected virtual void ConfigureCommand(DbCommand command)
    {
    }

    private async Task<(EventPage Page, int RowsRead)> loadRangeAsync(
        EventRequest request, long floor, long ceiling, long pageFloor, CancellationToken token)
    {
        var page = new EventPage(pageFloor);
        var skipped = 0;
        var rowsRead = 0;
        long? lastObserved = null;

        var command = Dialect.BuildPageCommand(QueryFor(floor, ceiling, request.BatchSize));

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var cmd = CreateCommand(connection, command);
        await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

        try
        {
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                rowsRead++;

                // Read off the row rather than out of a failure, so it is available for exactly the
                // page that most needs it: one whose every row was skipped because a whole event
                // type stopped resolving. See EventPage.LastObservedSequence.
                lastObserved = ReadSequence(reader);

                IEvent? @event;
                try
                {
                    @event = await ReadEventAsync(reader, token).ConfigureAwait(false);
                }
                catch (Exception e) when (ShouldSkip(e, request.ErrorOptions))
                {
                    skipped++;
                    await RecordSkippedEventAsync(e, request, token).ConfigureAwait(false);
                    continue;
                }

                if (@event is null)
                {
                    if (request.ErrorOptions.SkipUnknownEvents)
                    {
                        skipped++;
                        continue;
                    }

                    throw UnresolvedEventTypeException(reader, lastObserved.Value);
                }

                page.Add(@event);
            }
        }
        finally
        {
            await reader.CloseAsync().ConfigureAwait(false);
        }

        ReportLastObservedSequence(page, lastObserved);

        // The ceiling is calculated against the bound this query actually carried, which is not
        // necessarily the shard's high-water mark — see LoadByWindowAsync.
        page.CalculateCeiling(request.BatchSize, ceiling, skipped);

        return (page, rowsRead);
    }
}
