using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Npgsql;
using Shouldly;
using Weasel.EntityFrameworkCore.Batching;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

/// <summary>
///     BatchedQuery must return exactly what EF Core returns for the same query, whether it batches
///     (interceptor registered) or runs each query separately (no interceptor, split queries).
/// </summary>
public class batch_query_materialization_tests: IAsyncLifetime
{
    private readonly RoundTripCounter _roundTrips = new();
    private NpgsqlDataSource _dataSource = null!;
    private readonly Guid _orderId = Guid.NewGuid();
    private readonly Guid _otherOrderId = Guid.NewGuid();

    public async ValueTask InitializeAsync()
    {
        _dataSource = new NpgsqlDataSourceBuilder(BatchQueryDbContext.ConnectionString)
            .UseLoggerFactory(LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Debug).AddProvider(_roundTrips)))
            .Build();

        await using var context = CreateContext();
        await context.Database.ExecuteSqlRawAsync(
            $"DROP SCHEMA IF EXISTS {BatchQueryDbContext.TestSchema} CASCADE; CREATE SCHEMA {BatchQueryDbContext.TestSchema};");
        await context.GetService<IRelationalDatabaseCreator>().CreateTablesAsync();

        context.Orders.Add(NewOrder(_orderId, "c"));
        context.Orders.Add(NewOrder(_otherOrderId, "d"));
        await context.SaveChangesAsync();
    }

    public async ValueTask DisposeAsync() => await _dataSource.DisposeAsync();

    private static BatchOrder NewOrder(Guid id, string customer) => new(id, customer)
    {
        ShippingAddress = new BatchAddress { City = "Oslo" },
        Settings = new BatchSettings { Gift = true },
        Total = new BatchMoney { Amount = 42 },
        Discount = new BatchMoney { Amount = 5 },
        Tags = [new BatchTag { Name = "vip" }],
        Lines = [new BatchOrderLine { Id = Guid.NewGuid(), Sku = "A" }, new BatchOrderLine { Id = Guid.NewGuid(), Sku = "B" }]
    };

    private BatchQueryDbContext CreateContext(bool batching = true,
        QueryTrackingBehavior tracking = QueryTrackingBehavior.TrackAll)
    {
        var builder = new DbContextOptionsBuilder<BatchQueryDbContext>()
            .UseNpgsql(_dataSource)
            .UseQueryTrackingBehavior(tracking);

        if (batching) builder.UseWeaselBatchedQueries();

        return new BatchQueryDbContext(builder.Options);
    }

    private static void ShouldBeFullyLoaded(BatchOrder? order)
    {
        order.ShouldNotBeNull();
        order.ShippingAddress.City.ShouldBe("Oslo");
        order.Settings.Gift.ShouldBeTrue();
        order.Total.Amount.ShouldBe(42);
#if NET10_0_OR_GREATER
        order.Discount!.Amount.ShouldBe(5);
        order.Tags.Single().Name.ShouldBe("vip");
#endif
        order.Lines.Select(x => x.Sku).OrderBy(x => x).ShouldBe(["A", "B"]);
    }

    [Fact]
    public async Task batched_entities_are_fully_materialized_in_one_round_trip()
    {
        await using var context = CreateContext();
        await using var batch = context.CreateBatchQuery();

        var single = batch.QuerySingle(context.Orders.Include(x => x.Lines).Where(x => x.Id == _orderId));
        var list = batch.Query(context.Orders.Include(x => x.Lines).OrderBy(x => x.Customer));

        _roundTrips.Reset();
        await batch.ExecuteAsync();

        _roundTrips.Count.ShouldBe(1);
        ShouldBeFullyLoaded(await single);

        // A collection Include returns one entity per parent, not one per joined row
        var orders = await list;
        orders.Select(x => x.Id).ShouldBe([_orderId, _otherOrderId]);
        orders.ShouldAllBe(x => x.Lines.Count == 2);
    }

    [Fact]
    public async Task tracking_query_tracks_results_and_resolves_identity()
    {
        await using var context = CreateContext();
        var alreadyTracked = await context.Orders.SingleAsync(x => x.Id == _orderId);

        await using var batch = context.CreateBatchQuery();
        var single = batch.QuerySingle(context.Orders.Where(x => x.Id == _orderId));
        var other = batch.QuerySingle(context.Orders.Where(x => x.Id == _otherOrderId));
        await batch.ExecuteAsync();

        (await single).ShouldBeSameAs(alreadyTracked);
        context.Entry((await other)!).State.ShouldBe(EntityState.Unchanged);
    }

    [Fact]
    public async Task no_tracking_query_returns_untracked_results()
    {
        await using var context = CreateContext(tracking: QueryTrackingBehavior.NoTracking);
        await using var batch = context.CreateBatchQuery();

        var single = batch.QuerySingle(context.Orders.Include(x => x.Lines).Where(x => x.Id == _orderId));
        var tracked = batch.QuerySingle(context.Orders.AsTracking().Where(x => x.Id == _otherOrderId));
        await batch.ExecuteAsync();

        ShouldBeFullyLoaded(await single);
        context.Entry((await single)!).State.ShouldBe(EntityState.Detached);
        context.Entry((await tracked)!).State.ShouldBe(EntityState.Unchanged);
    }

    [Fact]
    public async Task saving_a_batched_entity_keeps_its_data()
    {
        BatchOrder? order;
        await using (var context = CreateContext(tracking: QueryTrackingBehavior.NoTracking))
        {
            await using var batch = context.CreateBatchQuery();
            var single = batch.QuerySingle(context.Orders.Where(x => x.Id == _orderId));
            await batch.ExecuteAsync();
            order = await single;
        }

        await using (var writer = CreateContext())
        {
            writer.Orders.Update(order!);
            await writer.SaveChangesAsync();
        }

        await using var reader = CreateContext();
        ShouldBeFullyLoaded(await reader.Orders.Include(x => x.Lines).SingleAsync(x => x.Id == _orderId));
    }

    [Fact]
    public async Task projections_and_scalars_can_be_batched()
    {
        await using var context = CreateContext();
        await using var batch = context.CreateBatchQuery();

        var summaries = batch.Query(context.Orders.OrderBy(x => x.Customer)
            .Select(x => new { x.Customer, x.ShippingAddress.City, LineCount = x.Lines.Count }));
        var lineCount = batch.Scalar(context.Orders.Where(x => x.Id == _orderId).Select(x => x.Lines.Count));
        var missing = batch.Scalar(context.Orders.Where(x => x.Customer == "nobody").Select(x => x.Total.Amount));

        _roundTrips.Reset();
        await batch.ExecuteAsync();

        _roundTrips.Count.ShouldBe(1);
        (await summaries).Select(x => (x.Customer, x.City, x.LineCount)).ShouldBe([("c", "Oslo", 2), ("d", "Oslo", 2)]);
        (await lineCount).ShouldBe(2);
        (await missing).ShouldBe(0);
    }

    [Fact]
    public async Task without_the_interceptor_each_query_runs_separately_with_the_same_results()
    {
        await using var context = CreateContext(batching: false);
        await using var batch = context.CreateBatchQuery();

        var single = batch.QuerySingle(context.Orders.Include(x => x.Lines).Where(x => x.Id == _orderId));
        var list = batch.Query(context.Orders.Include(x => x.Lines).OrderBy(x => x.Customer));

        _roundTrips.Reset();
        await batch.ExecuteAsync();

        _roundTrips.Count.ShouldBe(2);
        ShouldBeFullyLoaded(await single);
        (await list).Count.ShouldBe(2);
    }

    [Fact]
    public async Task split_queries_run_separately_and_the_rest_still_batch()
    {
        await using var context = CreateContext();
        await using var batch = context.CreateBatchQuery();

        var split = batch.QuerySingle(context.Orders.Include(x => x.Lines).AsSplitQuery().Where(x => x.Id == _orderId));
        var first = batch.QuerySingle(context.Orders.Where(x => x.Id == _orderId));
        var second = batch.QuerySingle(context.Orders.Where(x => x.Id == _otherOrderId));

        _roundTrips.Reset();
        await batch.ExecuteAsync();

        // One batch for the two single queries, then the split query's own two commands
        _roundTrips.Count.ShouldBe(3);
        ShouldBeFullyLoaded(await split);
        (await first)!.Id.ShouldBe(_orderId);
        (await second)!.Id.ShouldBe(_otherOrderId);
    }

    [Fact]
    public async Task a_failing_batch_faults_every_future()
    {
        await using var context = CreateContext();
        await using var batch = context.CreateBatchQuery();

        var divideByZero = batch.Query(context.Orders.Select(x => 1 / (x.Customer.Length - 1)));
        var neverRead = batch.Query(context.Orders);

        await Should.ThrowAsync<PostgresException>(() => batch.ExecuteAsync());

        // Nothing is left pending: awaiting any future throws instead of hanging
        divideByZero.IsFaulted.ShouldBeTrue();
        neverRead.IsFaulted.ShouldBeTrue();
    }

    /// <summary>Counts Npgsql command and batch executions, i.e. database round trips.</summary>
    private sealed class RoundTripCounter: ILoggerProvider
    {
        private int _count;
        public int Count => _count;
        public void Reset() => Interlocked.Exchange(ref _count, 0);

        public ILogger CreateLogger(string categoryName) => new Logger(categoryName == "Npgsql.Command" ? this : null);
        public void Dispose() { }

        private sealed class Logger(RoundTripCounter? counter): ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => counter != null;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                if (counter != null && formatter(state, exception).StartsWith("Executing"))
                {
                    Interlocked.Increment(ref counter._count);
                }
            }
        }
    }
}
