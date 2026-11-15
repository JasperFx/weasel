using System.Collections.Concurrent;
using Shouldly;
using Weasel.Core.Sequences;
using Xunit;

namespace Weasel.Core.Tests.Sequences;

/// <summary>
///     weasel#833: <see cref="HiloSequenceBase.NextLongAsync" /> used to hold the gate it shares
///     with <see cref="HiloSequenceBase.NextLong" /> across the await of
///     <see cref="HiloSequenceBase.AdvanceToNextHi" />, so a synchronous caller blocked a thread
///     on asynchronous work. Under thread-pool pressure that is starvation (marten#5969).
/// </summary>
public class hilo_synchronous_callers_never_wait_on_async_work
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    private static FakeHiloSequence Sequence(int maxLo = 3)
        => new("things", new HiloSettings { MaxLo = maxLo });

    [Fact]
    public async Task a_synchronous_caller_is_not_blocked_behind_an_asynchronous_advance()
    {
        var seq = Sequence(maxLo: 3);
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        seq.AsyncIo = _ => parked.Task;

        // Parked until the end of the test: under weasel#808 this held the shared gate.
        var asyncId = seq.NextLongAsync().AsTask();
        asyncId.IsCompleted.ShouldBeFalse();

        // On a thread of its own, so a regression times out here rather than hanging the runner.
        var syncIds = await Task.Run(() => new[] { seq.NextLong(), seq.NextLong(), seq.NextLong(), seq.NextLong() })
            .WaitAsync(Patience);

        asyncId.IsCompleted.ShouldBeFalse("the asynchronous advance is still parked");
        syncIds.ShouldBe([1L, 2L, 3L, 4L]);
        seq.SyncAdvanceCount.ShouldBe(2);

        parked.SetResult();

        // The parked advance reserved hi 2 when it resumed: its own block, so its own id.
        (await asyncId.WaitAsync(Patience)).ShouldBe(7);
    }

    [Fact]
    public async Task synchronous_callers_draw_thousands_of_ids_while_an_asynchronous_advance_is_parked()
    {
        const int maxLo = 5;
        const int workers = 4;
        const int perWorker = 500;

        var seq = Sequence(maxLo);
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        seq.AsyncIo = _ => parked.Task;

        // Several asynchronous callers, all waiting on the one parked advance.
        var asyncIds = Enumerable.Range(0, 3).Select(_ => seq.NextLongAsync().AsTask()).ToArray();

        var syncIds = new ConcurrentBag<long>();
        using var start = new Barrier(workers);

        // If a synchronous caller waited for the parked advance, nothing below would ever finish,
        // because the advance is only released after every worker is done.
        var sync = Enumerable.Range(0, workers).Select(_ => Task.Run(() =>
        {
            start.SignalAndWait(Patience);
            for (var i = 0; i < perWorker; i++)
            {
                syncIds.Add(seq.NextLong());
            }
        })).ToArray();

        await Task.WhenAll(sync).WaitAsync(TimeSpan.FromSeconds(60));
        asyncIds.ShouldAllBe(t => !t.IsCompleted);

        parked.SetResult();
        var all = syncIds.Concat(await Task.WhenAll(asyncIds).WaitAsync(Patience)).ToArray();

        all.Length.ShouldBe(workers * perWorker + asyncIds.Length);
        all.Distinct().Count().ShouldBe(all.Length, "an id was handed out twice");

        // The asynchronous callers shared one advance rather than reserving a block each.
        seq.AsyncAdvanceCount.ShouldBe(1);
        seq.SamePathOverlaps.ShouldBe(0);
    }

    [Fact]
    public async Task concurrent_callers_with_a_slow_asynchronous_advance_never_share_an_id()
    {
        const int maxLo = 4;
        const int workers = 8;
        const int perWorker = 150;
        const int total = workers * perWorker;

        var seq = Sequence(maxLo);
        seq.AsyncIo = ct => Task.Delay(2, ct);
        seq.SyncIo = () => Thread.Sleep(1);

        var ids = new ConcurrentBag<long>();
        using var start = new Barrier(workers);

        var tasks = Enumerable.Range(0, workers).Select(worker => Task.Run(async () =>
        {
            start.SignalAndWait(Patience);
            for (var i = 0; i < perWorker; i++)
            {
                ids.Add(worker % 2 == 0
                    ? seq.NextLong()
                    : await seq.NextLongAsync().ConfigureAwait(false));
            }
        })).ToArray();

        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60));

        ids.Count.ShouldBe(total);
        ids.Distinct().Count().ShouldBe(total, "an id was handed out twice");
        ids.Min().ShouldBeGreaterThan(0);
        ids.Max().ShouldBeLessThanOrEqualTo((long)seq.AdvanceCount * maxLo);
        seq.SamePathOverlaps.ShouldBe(0);
        seq.SyncAdvanceCount.ShouldBeGreaterThan(0);
        seq.AsyncAdvanceCount.ShouldBeGreaterThan(0);
    }

    [Fact]
    public void a_failed_attempt_does_not_throw_away_the_block_in_use()
    {
        // With a synchronous and an asynchronous advance able to overlap, one advance's failed
        // attempt (the database's negative "could not secure a hi") must not clobber the block
        // the other has just installed.
        var seq = Sequence(maxLo: 3);
        seq.NextLong().ShouldBe(1);

        seq.TrySetCurrentHiForTest(-1L).ShouldBeFalse();

        seq.CurrentHi.ShouldBe(0);
        seq.NextLong().ShouldBe(2);
        seq.AdvanceCount.ShouldBe(1);
    }

    [Fact]
    public async Task an_advance_that_installs_nothing_throws_rather_than_retrying_forever()
    {
        // Once advances no longer run under a lock every caller waits on, the base re-checks after
        // its own advance and goes again if another caller drained the block. An advance that
        // returns without installing anything would make that a loop of database round trips.
        var seq = Sequence(maxLo: 3);
        seq.InstallNothing = true;

        Should.Throw<InvalidOperationException>(() => seq.NextLong())
            .Message.ShouldContain(nameof(HiloSequenceBase.AdvanceToNextHi) + "Sync");
        await Should.ThrowAsync<InvalidOperationException>(async () => await seq.NextLongAsync());

        seq.AdvanceCount.ShouldBe(2);

        // Nothing is left held: a sequence that recovers carries on.
        seq.InstallNothing = false;
        seq.NextLong().ShouldBeGreaterThan(0);
        (await seq.NextLongAsync().AsTask().WaitAsync(Patience)).ShouldBeGreaterThan(0);
    }
}
