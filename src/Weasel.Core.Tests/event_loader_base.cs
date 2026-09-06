using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
/// The lifted <see cref="EventLoaderBase"/> (weasel#566), exercised over a real SQLite events table
/// so the paging, the SQL-level event-type filter and the ceiling arithmetic are checked against a
/// database rather than against a mock that agrees with them.
/// </summary>
/// <remarks>
/// SQLite is used because it needs no server, not because the base is SQLite-specific — the base
/// only ever touches <see cref="DbConnection"/> and <see cref="DbDataReader"/>, and the dialect
/// here renders the SQL the way a store's would.
/// </remarks>
public class event_loader_base: IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"weasel_loader_{Guid.NewGuid():n}.db");
    private readonly string _connectionString;

    public event_loader_base()
    {
        _connectionString = $"Data Source={_path}";

        using var conn = new SqliteConnection(_connectionString);
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            create table events (
                seq_id integer primary key,
                type text not null,
                is_archived integer not null default 0,
                tenant_id text not null default '*DEFAULT*'
            );
            """;
        cmd.ExecuteNonQuery();
    }

    public void Dispose()
    {
        SqliteConnection.ClearPool(new SqliteConnection(_connectionString));
        if (File.Exists(_path)) File.Delete(_path);
        GC.SuppressFinalize(this);
    }

    private void Append(params (long Sequence, string Type)[] rows) =>
        Append(rows.Select(x => (x.Sequence, x.Type, false, "*DEFAULT*")).ToArray());

    private void Append(params (long Sequence, string Type, bool Archived, string TenantId)[] rows)
    {
        using var conn = new SqliteConnection(_connectionString);
        conn.Open();

        foreach (var row in rows)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                "insert into events (seq_id, type, is_archived, tenant_id) values (@s, @t, @a, @tenant)";
            cmd.Parameters.AddWithValue("@s", row.Sequence);
            cmd.Parameters.AddWithValue("@t", row.Type);
            cmd.Parameters.AddWithValue("@a", row.Archived ? 1 : 0);
            cmd.Parameters.AddWithValue("@tenant", row.TenantId);
            cmd.ExecuteNonQuery();
        }
    }

    private static EventRequest RequestFor(long floor, long highWater, int batchSize = 10,
        bool skipUnknown = false, bool skipSerialization = false) => new()
    {
        Floor = floor,
        HighWater = highWater,
        BatchSize = batchSize,
        ErrorOptions = new ErrorHandlingOptions
        {
            SkipUnknownEvents = skipUnknown, SkipSerializationErrors = skipSerialization
        }
    };

    private TestLoader LoaderFor(EventLoaderOptions? options = null) =>
        new(_connectionString, options);

    // ----------------------------------------------------------------------------------------
    // The ordinary page read
    // ----------------------------------------------------------------------------------------

    [Fact]
    public async Task reads_the_range_in_sequence_order()
    {
        Append((1, "a"), (2, "b"), (3, "c"), (4, "d"));

        var page = await LoaderFor().LoadAsync(RequestFor(0, 10), CancellationToken.None);

        page.Select(x => x.Sequence).ShouldBe([1, 2, 3, 4]);
    }

    [Fact]
    public async Task the_floor_is_exclusive_and_the_ceiling_is_inclusive()
    {
        Append((1, "a"), (2, "b"), (3, "c"), (4, "d"));

        var page = await LoaderFor().LoadAsync(RequestFor(1, 3), CancellationToken.None);

        // Not 1 (the floor is where the shard already got to), and 3 is included.
        page.Select(x => x.Sequence).ShouldBe([2, 3]);
    }

    [Fact]
    public async Task archived_events_are_excluded_unless_asked_for()
    {
        Append((1, "a", false, "*DEFAULT*"), (2, "b", true, "*DEFAULT*"), (3, "c", false, "*DEFAULT*"));

        var excluded = await LoaderFor().LoadAsync(RequestFor(0, 10), CancellationToken.None);
        excluded.Select(x => x.Sequence).ShouldBe([1, 3]);

        var included = await LoaderFor(new EventLoaderOptions { IncludeArchivedEvents = true })
            .LoadAsync(RequestFor(0, 10), CancellationToken.None);
        included.Select(x => x.Sequence).ShouldBe([1, 2, 3]);
    }

    [Fact]
    public async Task a_tenant_scoped_loader_reads_only_that_tenant()
    {
        Append((1, "a", false, "blue"), (2, "a", false, "green"), (3, "a", false, "blue"));

        var page = await LoaderFor(new EventLoaderOptions { TenantId = "blue" })
            .LoadAsync(RequestFor(0, 10), CancellationToken.None);

        page.Select(x => x.Sequence).ShouldBe([1, 3]);
    }

    // ----------------------------------------------------------------------------------------
    // The event-type filter has to be SQL. See EventTypeAllowList.
    // ----------------------------------------------------------------------------------------

    [Fact]
    public async Task the_event_type_filter_is_applied_in_sql_not_after_hydration()
    {
        Append((1, "wanted"), (2, "unwanted"), (3, "wanted"), (4, "unwanted"));

        var loader = LoaderFor(new EventLoaderOptions { EventTypes = AllowList("wanted") });

        var page = await loader.LoadAsync(RequestFor(0, 10), CancellationToken.None);

        page.Select(x => x.Sequence).ShouldBe([1, 3]);

        // The discriminating assertion: the loader never hydrated a filtered row. A client-side
        // filter would produce the same page having read and discarded both "unwanted" rows, which
        // is exactly the regression polecat and fisher#153 exist to prevent.
        loader.HydratedTypes.ShouldBe(["wanted", "wanted"]);
    }

    [Fact]
    public async Task an_empty_allow_list_means_every_event_type()
    {
        Append((1, "a"), (2, "b"));

        EventTypeAllowList.All.IsEmpty.ShouldBeTrue();

        var page = await LoaderFor(new EventLoaderOptions { EventTypes = EventTypeAllowList.All })
            .LoadAsync(RequestFor(0, 10), CancellationToken.None);

        page.Count.ShouldBe(2);
    }

    // ----------------------------------------------------------------------------------------
    // Skip accounting and the ceiling
    // ----------------------------------------------------------------------------------------

    [Fact]
    public async Task a_page_that_does_not_fill_the_batch_claims_the_high_water_mark()
    {
        Append((1, "a"), (2, "b"));

        var page = await LoaderFor().LoadAsync(RequestFor(0, 500, batchSize: 10), CancellationToken.None);

        page.Ceiling.ShouldBe(500);
    }

    [Fact]
    public async Task a_full_page_claims_only_the_last_event_it_read()
    {
        Append((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"));

        var page = await LoaderFor().LoadAsync(RequestFor(0, 500, batchSize: 3), CancellationToken.None);

        page.Count.ShouldBe(3);
        page.Ceiling.ShouldBe(3);
    }

    [Fact]
    public async Task a_skipped_event_still_counts_toward_the_batch()
    {
        // Three rows, batch size three, one of which is skipped: the batch was saturated even
        // though only two events survived, so the ceiling must not claim the high-water mark.
        Append((1, "a"), (2, TestLoader.UnknownType), (3, "c"), (4, "d"));

        var page = await LoaderFor()
            .LoadAsync(RequestFor(0, 500, batchSize: 3, skipUnknown: true), CancellationToken.None);

        page.Count.ShouldBe(2);
        page.Ceiling.ShouldBe(3);
    }

    [Fact]
    public async Task an_unknown_event_type_throws_when_the_shard_does_not_skip_them()
    {
        Append((1, "a"), (2, TestLoader.UnknownType));

        var ex = await Should.ThrowAsync<TestUnknownEventTypeException>(
            () => LoaderFor().LoadAsync(RequestFor(0, 10), CancellationToken.None));

        ex.Sequence.ShouldBe(2);
    }

    [Fact]
    public async Task a_null_event_is_treated_as_an_unresolved_type()
    {
        // Fisher's row reader returns null rather than throwing; both spellings have to reach the
        // same SkipUnknownEvents decision.
        Append((1, "a"), (2, TestLoader.UnresolvableType), (3, "c"));

        var skipped = await LoaderFor()
            .LoadAsync(RequestFor(0, 10, skipUnknown: true), CancellationToken.None);
        skipped.Select(x => x.Sequence).ShouldBe([1, 3]);

        await Should.ThrowAsync<TestUnknownEventTypeException>(
            () => LoaderFor().LoadAsync(RequestFor(0, 10), CancellationToken.None));
    }

    [Fact]
    public async Task a_serialization_failure_is_governed_by_its_own_option()
    {
        Append((1, "a"), (2, TestLoader.PoisonType), (3, "c"));

        // SkipUnknownEvents does not cover it — the categories are deliberately separate, because
        // the operator response differs.
        await Should.ThrowAsync<TestDeserializationException>(
            () => LoaderFor().LoadAsync(RequestFor(0, 10, skipUnknown: true), CancellationToken.None));

        var page = await LoaderFor()
            .LoadAsync(RequestFor(0, 10, skipSerialization: true), CancellationToken.None);
        page.Select(x => x.Sequence).ShouldBe([1, 3]);
    }

    [Fact]
    public async Task a_skipped_event_is_reported_to_the_store()
    {
        Append((1, "a"), (2, TestLoader.PoisonType));

        var loader = LoaderFor();
        await loader.LoadAsync(RequestFor(0, 10, skipSerialization: true), CancellationToken.None);

        loader.Skipped.Count.ShouldBe(1);
        loader.Skipped.Single().ShouldBeOfType<TestDeserializationException>();
    }

    [Fact]
    public async Task classification_walks_the_inner_exception_chain()
    {
        // A store's exception transformer may have wrapped the exception that knows its category.
        // Inspecting only the outermost one is how marten#4720's timeout detection missed every
        // wrapped case for as long as it did.
        var wrapped = new InvalidOperationException("wrapped by the store",
            new TestDeserializationException(7));

        EventLoaderBase.Classify(wrapped).ShouldBe(ShardFailureCategory.EventSerialization);
        EventLoaderBase.Classify(new InvalidOperationException("nothing to see")).ShouldBeNull();
        EventLoaderBase.Classify(null).ShouldBeNull();
    }

    // ----------------------------------------------------------------------------------------
    // Skip-ahead
    // ----------------------------------------------------------------------------------------

    [Fact]
    public async Task the_probe_finds_the_first_matching_sequence_above_the_floor()
    {
        Append((1, "wanted"), (2, "unwanted"), (3, "unwanted"), (4, "wanted"));

        var loader = LoaderFor(new EventLoaderOptions { EventTypes = AllowList("wanted") });

        (await loader.ProbeAsync(0, 10)).ShouldBe(1);
        (await loader.ProbeAsync(1, 10)).ShouldBe(4);
        (await loader.ProbeAsync(4, 10)).ShouldBeNull();

        // Bounded above as well as below: the probe must not report a match outside the range it
        // was asked about.
        (await loader.ProbeAsync(1, 3)).ShouldBeNull();
    }

    [Fact]
    public async Task skip_ahead_jumps_the_floor_to_the_first_match()
    {
        Append((1, "unwanted"), (2, "unwanted"), (3, "unwanted"), (4, "wanted"), (5, "wanted"));

        var loader = LoaderFor(new EventLoaderOptions { EventTypes = AllowList("wanted") });

        var page = await loader.SkipAheadAsync(RequestFor(0, 500));

        page.Select(x => x.Sequence).ShouldBe([4, 5]);

        // The page's floor moves with the query's, because the probe PROVED there is no matching
        // event between the request's floor and the match.
        page.Floor.ShouldBe(3);
    }

    [Fact]
    public async Task skip_ahead_over_a_range_with_no_match_returns_an_empty_page_at_the_high_water_mark()
    {
        Append((1, "unwanted"), (2, "unwanted"));

        var loader = LoaderFor(new EventLoaderOptions { EventTypes = AllowList("wanted") });

        var page = await loader.SkipAheadAsync(RequestFor(0, 500));

        page.ShouldBeEmpty();
        page.Ceiling.ShouldBe(500);
    }

    // ----------------------------------------------------------------------------------------
    // Window-step. The ceiling is the whole point — marten#5239.
    // ----------------------------------------------------------------------------------------

    [Fact]
    public async Task window_step_reports_the_window_it_scanned_not_the_high_water_mark()
    {
        Append((5, "a"));

        var page = await LoaderFor().WindowAsync(RequestFor(0, 10_000, batchSize: 100), windowSize: 100);

        page.Select(x => x.Sequence).ShouldBe([5]);

        // 100, not 10_000. The query was bounded by the window, so that is the most this page can
        // honestly claim to have scanned; claiming the high-water mark would write durable progress
        // past every matching event between 100 and 10_000 and skip them permanently.
        page.Ceiling.ShouldBe(100);

        // And the page keeps the request's floor, unlike skip-ahead: this walk proved nothing about
        // the range below, it simply read it in slices.
        page.Floor.ShouldBe(0);
    }

    [Fact]
    public async Task window_step_walks_over_empty_windows_until_it_finds_rows()
    {
        Append((450, "a"), (451, "b"));

        var loader = LoaderFor();
        var page = await loader.WindowAsync(RequestFor(0, 10_000, batchSize: 100), windowSize: 100);

        page.Select(x => x.Sequence).ShouldBe([450, 451]);
        page.Ceiling.ShouldBe(500);

        // Five windows: (0,100], (100,200], (200,300], (300,400], (400,500].
        loader.PageQueries.Count.ShouldBe(5);
    }

    [Fact]
    public async Task window_step_returns_on_any_row_read_even_when_every_row_was_skipped()
    {
        // A window whose rows were all skipped may still have had matching events truncated by the
        // batch limit further up the same window, so advancing past it would drop them.
        Append((50, TestLoader.UnknownType), (150, "a"));

        var page = await LoaderFor().WindowAsync(
            RequestFor(0, 10_000, batchSize: 100, skipUnknown: true), windowSize: 100);

        page.ShouldBeEmpty();
        page.Ceiling.ShouldBe(100);
    }

    [Fact]
    public async Task an_exhausted_window_walk_claims_the_high_water_mark()
    {
        // Nothing anywhere in range: the walk really did scan every window, so claiming the
        // high-water mark here — as the loop must not — is honest.
        var page = await LoaderFor().WindowAsync(RequestFor(0, 500, batchSize: 100), windowSize: 100);

        page.ShouldBeEmpty();
        page.Ceiling.ShouldBe(500);
    }

    // ----------------------------------------------------------------------------------------
    // Strategy layering. The reason the base is primitives rather than one algorithm.
    // ----------------------------------------------------------------------------------------

    [Fact]
    public async Task a_store_can_layer_a_strategy_progression_over_the_primitives()
    {
        Append((1, "unwanted"), (2, "unwanted"), (3, "unwanted"), (4, "wanted"));

        var loader = new AdaptiveTestLoader(_connectionString,
            new EventLoaderOptions { EventTypes = AllowList("wanted") });

        // Normal: fails, escalates to skip-ahead.
        var page = await loader.LoadAsync(RequestFor(0, 500), CancellationToken.None);

        loader.Strategies.ShouldBe(["Normal", "SkipAhead"]);
        page.Select(x => x.Sequence).ShouldBe([4]);
    }

    // ----------------------------------------------------------------------------------------
    // Cancellation
    // ----------------------------------------------------------------------------------------

    [Fact]
    public async Task a_provider_error_under_cancellation_is_reported_as_cancellation()
    {
        // The daemon cancels a load mid-flight as a matter of course, and a provider does not
        // necessarily surface that as a clean OperationCanceledException. Reporting the raw
        // provider exception makes the daemon's recorder treat a routine shutdown as a shard error.
        Append((1, "a"));

        using var source = new CancellationTokenSource();
        var loader = new ThrowingLoader(_connectionString, source);

        await Should.ThrowAsync<OperationCanceledException>(
            () => loader.LoadAsync(RequestFor(0, 10), source.Token));
    }

    [Fact]
    public async Task an_error_with_no_cancellation_is_left_alone()
    {
        Append((1, "a"));

        var loader = new ThrowingLoader(_connectionString, cancelFirst: null);

        await Should.ThrowAsync<InvalidOperationException>(
            () => loader.LoadAsync(RequestFor(0, 10), CancellationToken.None));
    }

    // ----------------------------------------------------------------------------------------

    private static EventTypeAllowList AllowList(params string[] aliases)
    {
        var registry = new StubRegistry(aliases);
        return EventTypeAllowList.For(registry, aliases.Select((_, i) => StubRegistry.TypeAt(i)).ToList());
    }

    #region test doubles

    private sealed record TestEvent(string Type);

    /// <summary>Maps a handful of marker CLR types onto caller-chosen aliases.</summary>
    private sealed class StubRegistry: EventRegistry
    {
        private static readonly Type[] _types =
            [typeof(Marker0), typeof(Marker1), typeof(Marker2), typeof(Marker3)];

        private readonly string[] _aliases;

        public StubRegistry(string[] aliases) => _aliases = aliases;

        public static Type TypeAt(int index) => _types[index];

        public override IEventType EventMappingFor(Type eventType)
        {
            var index = Array.IndexOf(_types, eventType);
            return new EventTypeData<object>
            {
                EventTypeName = _aliases[index], DotNetTypeName = $"dotnet::{_aliases[index]}"
            };
        }

        private sealed record Marker0;
        private sealed record Marker1;
        private sealed record Marker2;
        private sealed record Marker3;
    }

    /// <summary>Renders SQLite SQL for the base's two commands.</summary>
    private sealed class SqliteTestDialect: IEventPagingDialect
    {
        public List<EventPageQuery> Queries { get; } = [];

        public EventPageCommand BuildPageCommand(EventPageQuery query)
        {
            Queries.Add(query);
            var (where, parameters) = Filters(query);

            return new EventPageCommand(
                $"select seq_id, type from events where {where} order by seq_id limit @batch",
                [.. parameters, new EventPageParameter("@batch", query.BatchSize, DbType.Int32)]);
        }

        public EventPageCommand BuildNextMatchingSequenceCommand(EventPageQuery query)
        {
            var (where, parameters) = Filters(query);

            // order by / limit 1, never min() -- see IEventPagingDialect.
            return new EventPageCommand(
                $"select seq_id from events where {where} order by seq_id limit 1", parameters);
        }

        private static (string Where, List<EventPageParameter> Parameters) Filters(EventPageQuery query)
        {
            var clauses = new List<string> { "seq_id > @floor", "seq_id <= @ceiling" };
            var parameters = new List<EventPageParameter>
            {
                new("@floor", query.Floor, DbType.Int64), new("@ceiling", query.Ceiling, DbType.Int64)
            };

            if (!query.IncludeArchivedEvents)
            {
                clauses.Add("is_archived = 0");
            }

            if (query.TenantId is not null)
            {
                clauses.Add("tenant_id = @tenant");
                parameters.Add(new EventPageParameter("@tenant", query.TenantId));
            }

            if (!query.EventTypes.IsEmpty)
            {
                // In SQL, never after hydration.
                var names = query.EventTypes.Aliases
                    .Select((_, i) => $"@type{i}")
                    .ToArray();
                clauses.Add($"type in ({string.Join(", ", names)})");
                parameters.AddRange(query.EventTypes.Aliases
                    .Select((alias, i) => new EventPageParameter($"@type{i}", alias)));
            }

            return (string.Join(" and ", clauses), parameters);
        }
    }

    private class TestLoader: EventLoaderBase
    {
        public const string UnknownType = "!unknown";
        public const string UnresolvableType = "!unresolvable";
        public const string PoisonType = "!poison";

        private readonly string _connectionString;
        private readonly SqliteTestDialect _dialect;

        public TestLoader(string connectionString, EventLoaderOptions? options)
            : this(connectionString, new SqliteTestDialect(), options)
        {
        }

        private TestLoader(string connectionString, SqliteTestDialect dialect, EventLoaderOptions? options)
            : base(dialect, options)
        {
            _connectionString = connectionString;
            _dialect = dialect;
        }

        public List<string> HydratedTypes { get; } = [];
        public List<Exception> Skipped { get; } = [];
        public IReadOnlyList<EventPageQuery> PageQueries => _dialect.Queries;

        public Task<long?> ProbeAsync(long floor, long ceiling) =>
            ProbeNextMatchingSequenceAsync(floor, ceiling, 10, CancellationToken.None);

        public Task<EventPage> SkipAheadAsync(EventRequest request) =>
            LoadWithSkipAheadAsync(request, CancellationToken.None);

        public Task<EventPage> WindowAsync(EventRequest request, long windowSize) =>
            LoadByWindowAsync(request, windowSize, CancellationToken.None);

        protected override async ValueTask<DbConnection> OpenConnectionAsync(CancellationToken token)
        {
            var conn = new SqliteConnection(_connectionString);
            await conn.OpenAsync(token);
            return conn;
        }

        protected override ValueTask<IEvent?> ReadEventAsync(DbDataReader reader, CancellationToken token)
        {
            var sequence = reader.GetInt64(0);
            var type = reader.GetString(1);

            HydratedTypes.Add(type);

            return type switch
            {
                UnknownType => throw new TestUnknownEventTypeException(sequence),
                PoisonType => throw new TestDeserializationException(sequence),
                UnresolvableType => ValueTask.FromResult<IEvent?>(null),
                _ => ValueTask.FromResult<IEvent?>(
                    new Event<TestEvent>(new TestEvent(type)) { Sequence = sequence, EventTypeName = type })
            };
        }

        protected override Exception UnresolvedEventTypeException(DbDataReader reader, long sequence) =>
            new TestUnknownEventTypeException(sequence);

        protected override ValueTask RecordSkippedEventAsync(
            Exception ex, EventRequest request, CancellationToken token)
        {
            Skipped.Add(ex);
            return default;
        }
    }

    /// <summary>
    /// A store layering an escalation strategy over the base's primitives — the shape Marten's
    /// loader has and the other two do not. Nothing about paging, skip accounting or the ceiling is
    /// reimplemented here.
    /// </summary>
    private sealed class AdaptiveTestLoader(string connectionString, EventLoaderOptions options)
        : TestLoader(connectionString, options)
    {
        public List<string> Strategies { get; } = [];

        public override async Task<EventPage> LoadAsync(EventRequest request, CancellationToken token)
        {
            Strategies.Add("Normal");

            try
            {
                throw new TimeoutException("statement timeout");
            }
            catch (TimeoutException)
            {
                Strategies.Add("SkipAhead");
                return await LoadWithSkipAheadAsync(request, token);
            }
        }
    }

    private sealed class ThrowingLoader(string connectionString, CancellationTokenSource? cancelFirst)
        : TestLoader(connectionString, null)
    {
        protected override ValueTask<IEvent?> ReadEventAsync(DbDataReader reader, CancellationToken token)
        {
            // Stand in for a provider that reports a cancelled command as its own exception type
            // rather than as an OperationCanceledException.
            cancelFirst?.Cancel();
            throw new InvalidOperationException("Operation cancelled by user.");
        }
    }

    private sealed class TestUnknownEventTypeException(long sequence)
        : Exception($"Unknown event type at {sequence}"), IEventFailureContext
    {
        public ShardFailureCategory Category => ShardFailureCategory.UnknownEventType;
        public long Sequence { get; } = sequence;
        public string? EventTypeName => null;
        public Guid? EventId => null;
        public Guid? StreamId => null;
        public string? StreamKey => null;
        public string? TenantId => null;
        public long? Version => null;
    }

    private sealed class TestDeserializationException(long sequence)
        : Exception($"Could not deserialize the event at {sequence}"), IEventFailureContext
    {
        public ShardFailureCategory Category => ShardFailureCategory.EventSerialization;
        public long Sequence { get; } = sequence;
        public string? EventTypeName => null;
        public Guid? EventId => null;
        public Guid? StreamId => null;
        public string? StreamKey => null;
        public string? TenantId => null;
        public long? Version => null;
    }

    #endregion
}
