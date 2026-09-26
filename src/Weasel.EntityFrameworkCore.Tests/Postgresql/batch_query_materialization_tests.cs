using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
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
        QueryTrackingBehavior tracking = QueryTrackingBehavior.TrackAll, bool retryOnFailure = false,
        IInterceptor[]? interceptors = null, IInterceptor[]? interceptorsAfterBatching = null)
    {
        var builder = new DbContextOptionsBuilder<BatchQueryDbContext>()
            .UseNpgsql(_dataSource, o =>
            {
                if (retryOnFailure) o.EnableRetryOnFailure();
            })
            .UseQueryTrackingBehavior(tracking)
            .AddInterceptors(interceptors ?? []);

        if (batching) builder.UseWeaselBatchedQueries();
        builder.AddInterceptors(interceptorsAfterBatching ?? []);

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
    public async Task a_split_query_makes_every_query_run_separately_with_the_same_results()
    {
        await using var context = CreateContext();
        await using var batch = context.CreateBatchQuery();

        var split = batch.QuerySingle(context.Orders.Include(x => x.Lines).AsSplitQuery().Where(x => x.Id == _orderId));
        var first = batch.QuerySingle(context.Orders.Where(x => x.Id == _orderId));
        var second = batch.QuerySingle(context.Orders.Where(x => x.Id == _otherOrderId));

        _roundTrips.Reset();
        await batch.ExecuteAsync();

        // The split query's own two commands, then one for each of the other queries
        _roundTrips.Count.ShouldBe(4);
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

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task captured_values_are_read_when_the_query_is_queued(bool batching)
    {
        await using var context = CreateContext(batching);
        await using var batch = context.CreateBatchQuery();

        string? customer = "c";
        var orders = batch.Query(context.Orders.Where(x => x.Customer == customer));
        customer = null;

        await batch.ExecuteAsync();

        (await orders).Single().Id.ShouldBe(_orderId);
    }

    [Fact]
    public async Task command_interceptors_apply_to_batched_queries()
    {
        // Registered before the batching interceptor, so EF Core runs it first
        await using var context = CreateContext(interceptors: [new ReplaceParameterValue("c", "d")]);
        await using var batch = context.CreateBatchQuery();

        // A variable, so EF Core sends it as a parameter the interceptor can change
        var customer = "c";
        var orders = batch.Query(context.Orders.Where(x => x.Customer == customer));
        await batch.ExecuteAsync();

        // Exactly what EF Core returns for the same query with the same interceptor
        (await orders).Single().Id.ShouldBe(_otherOrderId);
        (await context.Orders.Where(x => x.Customer == customer).SingleAsync()).Id.ShouldBe(_otherOrderId);
    }

    [Fact]
    public async Task a_retrying_execution_strategy_recovers_from_a_transient_error()
    {
        // Fails after the batch has handed EF Core the query's result set, like an error while reading it
        var probe = new FailOnceWithTransientError();
        await using var context = CreateContext(retryOnFailure: true, interceptorsAfterBatching: [probe]);
        await using var batch = context.CreateBatchQuery();

        var first = batch.QuerySingle(context.Orders.Where(x => x.Id == _orderId));
        var second = batch.QuerySingle(context.Orders.Where(x => x.Id == _otherOrderId));

        probe.Armed = true;
        await batch.ExecuteAsync();

        probe.Failed.ShouldBeTrue();
        (await first)!.Id.ShouldBe(_orderId);
        (await second)!.Id.ShouldBe(_otherOrderId);
    }

    [Fact]
    public async Task a_retrying_execution_strategy_runs_each_query_separately()
    {
        await using var context = CreateContext(retryOnFailure: true);
        await using var batch = context.CreateBatchQuery();

        var first = batch.QuerySingle(context.Orders.Where(x => x.Id == _orderId));
        var second = batch.QuerySingle(context.Orders.Where(x => x.Id == _otherOrderId));

        _roundTrips.Reset();
        await batch.ExecuteAsync();

        _roundTrips.Count.ShouldBe(2);
        (await first)!.Id.ShouldBe(_orderId);
        (await second)!.Id.ShouldBe(_otherOrderId);
    }

    [Fact]
    public async Task queries_run_in_the_order_they_were_queued()
    {
        await using var context = CreateContext();
        await using var batch = context.CreateBatchQuery();

        var split = batch.Query(context.Orders.TagWith("first").Include(x => x.Lines).AsSplitQuery());
        var single = batch.QuerySingle(context.Orders.TagWith("second").Where(x => x.Id == _orderId));

        _roundTrips.Reset();
        await batch.ExecuteAsync();

        var commands = _roundTrips.Commands;
        commands.FindIndex(x => x.Contains("-- first")).ShouldBeLessThan(commands.FindIndex(x => x.Contains("-- second")));
        (await split).Count.ShouldBe(2);
        (await single)!.Id.ShouldBe(_orderId);
    }

    /// <summary>Rewrites a parameter value before EF Core executes a command.</summary>
    private sealed class ReplaceParameterValue(object from, object to): DbCommandInterceptor
    {
        private void replace(DbCommand command)
        {
            foreach (DbParameter parameter in command.Parameters)
            {
                if (Equals(parameter.Value, from)) parameter.Value = to;
            }
        }

        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData,
            InterceptionResult<DbDataReader> result)
        {
            replace(command);
            return result;
        }

        public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command,
            CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
        {
            replace(command);
            return new(result);
        }
    }

    /// <summary>Fails the first query after being armed with an error the retrying strategy treats as transient.</summary>
    private sealed class FailOnceWithTransientError: DbCommandInterceptor
    {
        public bool Armed { get; set; }
        public bool Failed { get; private set; }

        private void maybeFail()
        {
            if (!Armed || Failed) return;
            Failed = true;
            throw new NpgsqlException("Injected transient error", new TimeoutException());
        }

        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData,
            InterceptionResult<DbDataReader> result)
        {
            maybeFail();
            return result;
        }

        public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command,
            CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
        {
            maybeFail();
            return new(result);
        }
    }

    /// <summary>Counts Npgsql command and batch executions, i.e. database round trips.</summary>
    private sealed class RoundTripCounter: ILoggerProvider
    {
        private int _count;
        public int Count => _count;
        public List<string> Commands { get; } = [];

        public void Reset()
        {
            Interlocked.Exchange(ref _count, 0);
            lock (Commands) Commands.Clear();
        }

        public ILogger CreateLogger(string categoryName) => new Logger(categoryName == "Npgsql.Command" ? this : null);
        public void Dispose() { }

        private sealed class Logger(RoundTripCounter? counter): ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => counter != null;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                var message = formatter(state, exception);
                if (counter != null && message.StartsWith("Executing"))
                {
                    Interlocked.Increment(ref counter._count);
                    lock (counter.Commands) counter.Commands.Add(message);
                }
            }
        }
    }
}
