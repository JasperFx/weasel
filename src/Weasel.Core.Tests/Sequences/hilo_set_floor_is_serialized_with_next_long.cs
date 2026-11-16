using System.Collections.Concurrent;
using Shouldly;
using Weasel.Core.Sequences;
using Xunit;

namespace Weasel.Core.Tests.Sequences;

/// <summary>
///     weasel#849: an install from outside <see cref="HiloSequenceBase.NextLong" /> (a subclass's
///     <see cref="HiloSequenceBase.SetFloor" />, or <see cref="HiloSequenceBase.AdvanceToNextHi" />
///     called directly) is serialized with <see cref="HiloSequenceBase.NextLong" />.
/// </summary>
public class hilo_set_floor_is_serialized_with_next_long
{
    /// <summary>
    ///     Stands in for a dialect over a shared hilo row: every reservation is an atomic increment,
    ///     so each advance gets a block of its own, and <see cref="SetFloor" /> moves the row and then
    ///     installs a block reserved above it, as Marten's does.
    /// </summary>
    private class RowBackedHiloSequence: HiloSequenceBase
    {
        private long _row = -1;

        public RowBackedHiloSequence(int maxLo): base("things", new HiloSettings { MaxLo = maxLo })
        {
        }

        /// <summary>
        ///     When set, the next synchronous advance reserves its block, signals
        ///     <see cref="Reserved" />, and does not install it until <see cref="Release" /> is set.
        /// </summary>
        public bool HoldNextSyncAdvance { get; set; }

        public ManualResetEventSlim Reserved { get; } = new();
        public ManualResetEventSlim Release { get; } = new();

        private long reserve() => Interlocked.Increment(ref _row);

        public override Task AdvanceToNextHi(CancellationToken ct = default)
        {
            TrySetCurrentHi(reserve());
            return Task.CompletedTask;
        }

        protected override void AdvanceToNextHiSync()
        {
            var hi = reserve();

            if (HoldNextSyncAdvance)
            {
                HoldNextSyncAdvance = false;
                Reserved.Set();
                Release.Wait(TimeSpan.FromSeconds(30)).ShouldBeTrue();
            }

            TrySetCurrentHi(hi);
        }

        public override Task SetFloor(long floor)
        {
            var pages = (long)Math.Ceiling((double)floor / MaxLo);
            Interlocked.Exchange(ref _row, pages);
            TrySetCurrentHi(reserve());
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task an_advance_in_flight_cannot_undo_set_floor()
    {
        const long floor = 1000;
        var seq = new RowBackedHiloSequence(maxLo: 2);

        // Use up the first block, so the next NextLong has to advance.
        seq.NextLong().ShouldBe(1);
        seq.NextLong().ShouldBe(2);

        // That advance reserves its block under the old floor, then stalls before installing it.
        seq.HoldNextSyncAdvance = true;
        var inFlight = Task.Run(() => seq.NextLong());
        seq.Reserved.Wait(TimeSpan.FromSeconds(30)).ShouldBeTrue();

        // The floor moves while it is in flight. Give SetFloor every chance to finish first: before
        // weasel#849 it did, and the stalled advance then installed its old block on top of the floor's.
        var setFloor = Task.Run(() => seq.SetFloor(floor));
        await Task.WhenAny(setFloor, Task.Delay(500));

        seq.Release.Set();
        var issuedByTheAdvanceInFlight = await inFlight;
        await setFloor;

        // The call that was already part way through its advance when the floor moved is allowed
        // its old block; it started first.
        issuedByTheAdvanceInFlight.ShouldBeLessThan(floor);

        // Every id after SetFloor has returned is above the floor. These were 3 and 4 against
        // Marten 9.0 (marten#5998).
        var after = new[] { seq.NextLong(), seq.NextLong(), seq.NextLong() };
        after.ShouldAllBe(id => id > floor, $"ids after SetFloor({floor}) returned: {string.Join(", ", after)}");
    }

    [Fact]
    public async Task installs_from_outside_next_long_never_produce_a_duplicate_id()
    {
        // An install racing NextLong's AdvanceValue could leave it reading one block's hi and the
        // other's lo, or have CurrentLo++ overwrite the reset to 1, and repeat an id. The database
        // hands every reservation a block of its own, so any duplicate here is the client's doing.
        var seq = new RowBackedHiloSequence(maxLo: 50);
        var ids = new ConcurrentBag<long>();
        using var stop = new CancellationTokenSource();

        var installer = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                await seq.AdvanceToNextHi();
            }
        });

        var issuers = Enumerable.Range(0, Math.Max(4, Environment.ProcessorCount / 2))
            .Select(_ => Task.Run(() =>
            {
                for (var i = 0; i < 20_000; i++)
                {
                    ids.Add(seq.NextLong());
                }
            }))
            .ToArray();

        await Task.WhenAll(issuers);
        await stop.CancelAsync();
        await installer;

        var duplicates = ids.GroupBy(x => x).Where(g => g.Count() > 1).Select(g => g.Key).Take(10).ToList();
        duplicates.ShouldBeEmpty($"duplicate ids: {string.Join(", ", duplicates)}");
    }
}
