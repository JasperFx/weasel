using System.Collections.Concurrent;
using System.Reflection;
using Shouldly;
using Weasel.Core.Sequences;
using Xunit;

namespace Weasel.Core.Tests.Sequences;

/// <summary>
///     weasel#808: <see cref="HiloSequenceBase.NextLongAsync" /> fetches the next hi through the
///     asynchronous <see cref="HiloSequenceBase.AdvanceToNextHi" />, and shares one gate with the
///     synchronous <see cref="HiloSequenceBase.NextLong" /> so the two can be mixed on one instance.
/// </summary>
public class hilo_sequence_issues_ids_asynchronously
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    private static FakeHiloSequence Sequence(int maxLo = 3)
        => new("things", new HiloSettings { MaxLo = maxLo });

    [Fact]
    public async Task issues_the_same_contiguous_run_as_the_synchronous_path()
    {
        var seq = Sequence(maxLo: 3);

        (await seq.NextLongAsync()).ShouldBe(1);
        (await seq.NextLongAsync()).ShouldBe(2);
        (await seq.NextLongAsync()).ShouldBe(3);

        // Exhausted: hi advances to 1 -> (1*3)+1 = 4
        (await seq.NextLongAsync()).ShouldBe(4);

        seq.AdvanceCount.ShouldBe(2);
    }

    [Fact]
    public async Task advances_through_the_asynchronous_io_and_never_the_synchronous()
    {
        var seq = Sequence(maxLo: 2);

        for (var i = 0; i < 5; i++)
        {
            await seq.NextLongAsync();
        }

        seq.AsyncAdvanceCount.ShouldBe(3);
        seq.SyncAdvanceCount.ShouldBe(0);
    }

    [Fact]
    public async Task completes_synchronously_while_the_allocation_has_room()
    {
        var seq = Sequence(maxLo: 10);
        await seq.NextLongAsync();

        var next = seq.NextLongAsync().AsTask();

        next.IsCompletedSuccessfully.ShouldBeTrue();
        (await next).ShouldBe(2);
    }

    [Fact]
    public async Task does_not_issue_an_id_until_the_hi_has_been_fetched()
    {
        var seq = Sequence(maxLo: 3);
        var fetched = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        seq.AsyncIo = _ => fetched.Task;

        var pending = seq.NextLongAsync().AsTask();
        pending.IsCompleted.ShouldBeFalse();
        seq.CurrentHi.ShouldBe(-1);

        fetched.SetResult();

        (await pending.WaitAsync(Patience)).ShouldBe(1);
    }

    [Fact]
    public async Task synchronous_and_asynchronous_callers_draw_from_one_allocation()
    {
        var seq = Sequence(maxLo: 3);

        seq.NextLong().ShouldBe(1);
        (await seq.NextLongAsync()).ShouldBe(2);
        seq.NextLong().ShouldBe(3);
        (await seq.NextLongAsync()).ShouldBe(4);
        seq.NextLong().ShouldBe(5);

        seq.SyncAdvanceCount.ShouldBe(1);
        seq.AsyncAdvanceCount.ShouldBe(1);
    }

    [Fact]
    public async Task next_int_async_is_a_narrowing_cast_of_next_long_async()
    {
        var seq = Sequence(maxLo: 1000);

        (await seq.NextIntAsync()).ShouldBe(1);
        (await seq.NextIntAsync()).ShouldBe(2);
        seq.NextInt().ShouldBe(3);
    }

    [Fact]
    public async Task an_already_cancelled_token_issues_no_id()
    {
        var seq = Sequence(maxLo: 3);
        seq.NextLong().ShouldBe(1);

        await Should.ThrowAsync<OperationCanceledException>(
            async () => await seq.NextLongAsync(new CancellationToken(true)));

        // Nothing was consumed, so the run carries on where it was.
        (await seq.NextLongAsync()).ShouldBe(2);
    }

    [Fact]
    public async Task an_already_cancelled_token_does_not_advance_the_hi()
    {
        var seq = Sequence(maxLo: 3);

        await Should.ThrowAsync<OperationCanceledException>(
            async () => await seq.NextLongAsync(new CancellationToken(true)));

        seq.AdvanceCount.ShouldBe(0);
        seq.CurrentHi.ShouldBe(-1);
    }

    [Fact]
    public async Task cancelling_a_caller_waiting_for_the_gate_consumes_nothing()
    {
        var seq = Sequence(maxLo: 3);
        var fetched = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        seq.AsyncIo = _ => fetched.Task;

        // The first caller holds the gate while its advance is in flight.
        var first = seq.NextLongAsync().AsTask();

        using var cts = new CancellationTokenSource();
        var waiting = seq.NextLongAsync(cts.Token).AsTask();
        waiting.IsCompleted.ShouldBeFalse();

        await cts.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(() => waiting.WaitAsync(Patience));

        fetched.SetResult();
        (await first.WaitAsync(Patience)).ShouldBe(1);
        (await seq.NextLongAsync()).ShouldBe(2);
        seq.AdvanceCount.ShouldBe(1);
    }

    [Fact]
    public async Task the_token_reaches_the_advance_and_a_cancelled_advance_frees_the_gate()
    {
        var seq = Sequence(maxLo: 3);
        CancellationToken seen = default;
        seq.AsyncIo = ct =>
        {
            seen = ct;
            return Task.Delay(Timeout.Infinite, ct);
        };

        using var cts = new CancellationTokenSource();
        var pending = seq.NextLongAsync(cts.Token).AsTask();
        seen.ShouldBe(cts.Token);

        await cts.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(() => pending.WaitAsync(Patience));

        // The gate was released and no hi was applied: the next caller fetches it.
        seq.AsyncIo = null;
        (await seq.NextLongAsync().AsTask().WaitAsync(Patience)).ShouldBe(1);
        seq.NextLong().ShouldBe(2);
    }

    [Fact]
    public async Task a_failed_advance_frees_the_gate_for_both_paths()
    {
        var seq = Sequence(maxLo: 3);
        seq.AsyncIo = _ => Task.FromException(new InvalidOperationException("database unavailable"));

        await Should.ThrowAsync<InvalidOperationException>(async () => await seq.NextLongAsync());

        seq.AsyncIo = null;
        seq.NextLong().ShouldBe(1);
        (await seq.NextLongAsync().AsTask().WaitAsync(Patience)).ShouldBe(2);
    }

    [Fact]
    public async Task concurrent_synchronous_and_asynchronous_callers_never_share_an_id()
    {
        const int maxLo = 7;
        const int workers = 8;
        const int perWorker = 400;
        const int total = workers * perWorker;

        var seq = Sequence(maxLo);
        seq.AsyncIo = async _ => await Task.Yield();
        seq.SyncIo = () => Thread.Yield();

        var ids = new ConcurrentBag<long>();
        using var start = new Barrier(workers);

        var tasks = Enumerable.Range(0, workers).Select(worker => Task.Run(async () =>
        {
            start.SignalAndWait(Patience);
            for (var i = 0; i < perWorker; i++)
            {
                ids.Add((worker + i) % 2 == 0
                    ? seq.NextLong()
                    : await seq.NextLongAsync().ConfigureAwait(false));
            }
        })).ToArray();

        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60));

        ids.Count.ShouldBe(total);
        ids.Distinct().Count().ShouldBe(total, "an id was handed out twice");

        // Every advance was used to the end before the next one, so the ids are exactly
        // 1..total and there was exactly one advance per block: no double advance
        // skipped a block, and no two advances fetched the same hi.
        ids.Min().ShouldBe(1);
        ids.Max().ShouldBe(total);
        seq.AdvanceCount.ShouldBe((int)Math.Ceiling((double)total / maxLo));

        // Both paths really did advance, so the test covered the mix it claims to.
        seq.SyncAdvanceCount.ShouldBeGreaterThan(0);
        seq.AsyncAdvanceCount.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task a_synchronous_caller_on_a_single_threaded_context_does_not_deadlock_an_async_advance()
    {
        // The async advance below awaits with ConfigureAwait(true). If it captured the
        // context, its continuation would queue to the one thread that the synchronous
        // NextLong is blocking on the gate, and neither would ever finish.
        var seq = Sequence(maxLo: 3);
        seq.AsyncIo = _ => Task.Delay(50);

        using var context = new SingleThreadedContext();
        var outcome = new TaskCompletionSource<(Task<long> Async, long Sync)>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        context.Post(_ =>
        {
            try
            {
                var async = seq.NextLongAsync().AsTask();
                var sync = seq.NextLong();
                outcome.SetResult((async, sync));
            }
            catch (Exception e)
            {
                outcome.SetException(e);
            }
        }, null);

        var (asyncId, syncId) = await outcome.Task.WaitAsync(Patience);
        (await asyncId.WaitAsync(Patience)).ShouldBe(1);
        syncId.ShouldBe(2);
    }

    [Fact]
    public async Task a_stale_implementer_gets_ids_from_its_own_next_long()
    {
        ISequence seq = new SequenceFromAnEarlierWeasel();

        (await seq.NextLongAsync()).ShouldBe(1);
        (await seq.NextIntAsync()).ShouldBe(2);
        seq.NextLong().ShouldBe(3);
    }

    [Fact]
    public async Task a_stale_implementer_honours_an_already_cancelled_token_without_issuing_an_id()
    {
        var stale = new SequenceFromAnEarlierWeasel();
        ISequence seq = stale;

        await Should.ThrowAsync<OperationCanceledException>(
            async () => await seq.NextLongAsync(new CancellationToken(true)));

        stale.Calls.ShouldBe(0);
    }

    [Fact]
    public async Task a_stale_implementers_failure_arrives_as_a_faulted_task()
    {
        ISequence seq = new SequenceFromAnEarlierWeasel { Failure = new InvalidOperationException("boom") };

        // Taking the task must not throw (a throw here fails the test); awaiting it does.
        var pending = seq.NextLongAsync().AsTask();

        pending.IsFaulted.ShouldBeTrue();
        await Should.ThrowAsync<InvalidOperationException>(() => pending);
    }

    [Theory]
    [InlineData(nameof(ISequence.NextLongAsync))]
    [InlineData(nameof(ISequence.NextIntAsync))]
    public void the_new_interface_members_carry_a_default_body(string member)
    {
        // weasel#682: an abstract interface member is a TypeLoadException for every
        // implementer compiled before it existed.
        var method = typeof(ISequence).GetMethod(member, BindingFlags.Public | BindingFlags.Instance);

        method.ShouldNotBeNull();
        method.IsAbstract.ShouldBeFalse();
    }

    /// <summary>
    ///     An <see cref="ISequence" /> as written before weasel#808: it knows nothing of the
    ///     asynchronous members, so it gets the interface defaults.
    /// </summary>
    private sealed class SequenceFromAnEarlierWeasel: ISequence
    {
        private long _current;

        public int Calls { get; private set; }
        public Exception? Failure { get; init; }

        public int MaxLo => 1;
        public int NextInt() => (int)NextLong();

        public long NextLong()
        {
            Calls++;
            if (Failure != null)
            {
                throw Failure;
            }

            return ++_current;
        }

        public Task SetFloor(long floor) => Task.CompletedTask;
    }

    /// <summary>
    ///     One dedicated thread draining a queue: the shape of a UI or legacy ASP.NET context,
    ///     where a continuation can only run on the thread that may be blocked.
    /// </summary>
    private sealed class SingleThreadedContext: SynchronizationContext, IDisposable
    {
        private readonly BlockingCollection<(SendOrPostCallback Callback, object? State)> _queue = new();

        public SingleThreadedContext()
        {
            // Background, so a deadlocked run cannot keep the test process alive.
            var thread = new Thread(pump) { IsBackground = true, Name = "single-threaded-context" };
            thread.Start();
        }

        public override void Post(SendOrPostCallback d, object? state) => _queue.Add((d, state));

        public override void Send(SendOrPostCallback d, object? state) =>
            throw new NotSupportedException();

        public void Dispose() => _queue.CompleteAdding();

        private void pump()
        {
            SetSynchronizationContext(this);
            foreach (var (callback, state) in _queue.GetConsumingEnumerable())
            {
                callback(state);
            }
        }
    }
}
