using System.Collections.Concurrent;
using Shouldly;
using Weasel.Core.Sequences;
using Xunit;

namespace Weasel.Core.Tests.Sequences;

/// <summary>
///     Since weasel#833 a synchronous and an asynchronous advance can overlap, and each reserves a
///     block of its own. The database hands out the hi when the statement runs, so the advance that
///     started first can come back with the <i>lower</i> block after the other has installed a
///     higher one and callers have drawn ids from it. 10.0.0-alpha.3 installed it anyway, and ids
///     went backwards: marten#5984's CI saw one caller get 2137 and then 2134. No id was issued
///     twice, but Marten documents a sequence's ids as unique <i>and increasing</i>.
/// </summary>
public class hilo_ids_only_increase_across_an_overlap
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    private static FakeHiloSequence Sequence(int maxLo = 3)
        => new("things", new HiloSettings { MaxLo = maxLo }) { ReserveBeforeAsyncIo = true };

    [Fact]
    public async Task a_lower_block_that_arrives_after_a_higher_one_is_dropped()
    {
        var seq = Sequence(maxLo: 3);
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        seq.AsyncIo = _ => parked.Task;

        // Reserves hi 0, then parks on the way back.
        var asyncId = seq.NextLongAsync().AsTask();
        asyncId.IsCompleted.ShouldBeFalse();

        // Reserves hi 1 and installs it while the asynchronous advance is still out.
        var first = await Task.Run(() => new[] { seq.NextLong(), seq.NextLong() }).WaitAsync(Patience);
        first.ShouldBe([4L, 5L]);

        // hi 0 comes back. Installing it would issue 1 here, behind the 4 and 5 already handed out.
        parked.SetResult();
        (await asyncId.WaitAsync(Patience)).ShouldBe(6);

        seq.CurrentHi.ShouldBe(1);
        seq.NextLong().ShouldBe(7); // hi 2: the rest of hi 0 is a gap
        seq.AdvanceCount.ShouldBe(3);
    }

    [Fact]
    public async Task concurrent_sync_and_async_callers_each_see_their_ids_increase()
    {
        // The shape of Marten's hilo_prefetch_tests.concurrent_sync_and_async_callers_never_get_the_same_id.
        const int maxLo = 3;
        const int callers = 16;
        const int idsPerCaller = 300;

        var seq = Sequence(maxLo);
        seq.AsyncIo = ct => Task.Delay(1, ct);
        seq.SyncIo = () => Thread.SpinWait(200);

        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = Enumerable.Range(0, callers).Select(caller => Task.Run(async () =>
        {
            await start.Task;
            var ids = new List<long>(idsPerCaller);
            for (var i = 0; i < idsPerCaller; i++)
            {
                ids.Add(caller % 2 == 0 || i % 3 != 0 ? seq.NextLong() : await seq.NextLongAsync());
            }

            return ids;
        })).ToArray();

        start.SetResult();
        var perCaller = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60));
        var all = perCaller.SelectMany(x => x).ToArray();

        all.Distinct().Count().ShouldBe(callers * idsPerCaller, "an id was handed out twice");
        foreach (var ids in perCaller)
        {
            ids.ShouldBe(ids.OrderBy(x => x).ToList(), customMessage: "a caller's ids went backwards");
        }

        seq.SamePathOverlaps.ShouldBe(0);
        seq.SyncAdvanceCount.ShouldBeGreaterThan(0);
        seq.AsyncAdvanceCount.ShouldBeGreaterThan(0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task without_an_overlap_a_lower_block_is_still_installed(bool asynchronously)
    {
        // A hilo row reset below the sequence (a cleaned database the instance outlived) must not
        // turn into a sequence that can never advance: the drop applies only across an overlap.
        var seq = Sequence(maxLo: 2);
        seq.ResetDatabaseTo(10);
        seq.NextLong().ShouldBe(21);
        seq.NextLong().ShouldBe(22);

        seq.ResetDatabaseTo(3);
        var next = asynchronously ? await seq.NextLongAsync() : seq.NextLong();

        next.ShouldBe(7);
        seq.CurrentHi.ShouldBe(3);
    }

    [Fact]
    public void outside_an_advance_a_lower_value_is_installed_as_given()
    {
        var seq = Sequence(maxLo: 3);
        seq.ResetDatabaseTo(5);
        seq.NextLong().ShouldBe(16);

        seq.TrySetCurrentHiForTest(2L).ShouldBeTrue();

        seq.CurrentHi.ShouldBe(2);
        seq.NextLong().ShouldBe(7);
    }
}
