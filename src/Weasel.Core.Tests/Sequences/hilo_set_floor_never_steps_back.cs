using Shouldly;
using Weasel.Core.Sequences;
using Xunit;

namespace Weasel.Core.Tests.Sequences;

/// <summary>
///     weasel#849, the 10.0 half. A <see cref="HiloSequenceBase.SetFloor" /> installs from outside
///     an advance, so <see cref="HiloSequenceBase" /> cannot tell when its block was reserved and
///     installs it as given. An advance that reserves after the hilo row moved, and installs first,
///     holds a higher block; the floor's lower block then replaced it, and ids stepped backwards
///     (above the floor and unique, but out of order). Passing the <c>InstallCount</c> taken before
///     the reservation to <c>TrySetCurrentHi(raw, installCountBefore)</c> applies the overlap rule an
///     advance gets.
/// </summary>
public class hilo_set_floor_never_steps_back
{
    /// <summary>
    ///     Shaped like Marten's <c>HiloSequence.SetFloor</c>: move the row, reserve a block above it,
    ///     install it. <see cref="BetweenReserveAndInstall" /> stands in for the round trip back, during
    ///     which another caller can advance.
    /// </summary>
    private sealed class FloorSequence: HiloSequenceBase
    {
        private long _row = -1;

        public FloorSequence(int maxLo): base("things", new HiloSettings { MaxLo = maxLo })
        {
        }

        public bool UseInstallCount { get; set; } = true;

        public Action? BetweenReserveAndInstall { get; set; }

        public long Installs => InstallCount;

        public void ResetDatabaseTo(long nextHi) => Interlocked.Exchange(ref _row, nextHi - 1);

        private long reserve() => Interlocked.Increment(ref _row);

        public override Task AdvanceToNextHi(CancellationToken ct = default)
        {
            TrySetCurrentHi(reserve());
            return Task.CompletedTask;
        }

        protected override void AdvanceToNextHiSync() => TrySetCurrentHi(reserve());

        public override Task SetFloor(long floor)
        {
            var pages = (long)Math.Ceiling((double)floor / MaxLo);
            Interlocked.Exchange(ref _row, pages);

            var before = InstallCount;
            var hi = reserve();

            var hook = BetweenReserveAndInstall;
            BetweenReserveAndInstall = null;
            hook?.Invoke();

            if (UseInstallCount)
            {
                TrySetCurrentHi(hi, before);
            }
            else
            {
                TrySetCurrentHi(hi);
            }

            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task an_advance_that_installs_a_higher_block_first_is_not_replaced_by_the_floors()
    {
        const long floor = 1000;
        var seq = new FloorSequence(maxLo: 2);
        seq.NextLong().ShouldBe(1);
        seq.NextLong().ShouldBe(2);

        // SetFloor reserves hi 501 (ids 1003, 1004). Before it installs it, an advance reserves
        // hi 502 and issues 1005 from it.
        var issuedDuringSetFloor = 0L;
        seq.BetweenReserveAndInstall = () => issuedDuringSetFloor = seq.NextLong();

        await seq.SetFloor(floor);

        issuedDuringSetFloor.ShouldBe(1005);

        // Installing 501 now would hand out 1003 and 1004 after 1005.
        seq.CurrentHi.ShouldBe(502);
        seq.NextLong().ShouldBe(1006);
        seq.NextLong().ShouldBe(1007); // hi 503: the floor's block is a gap
    }

    [Fact]
    public async Task without_an_install_in_between_the_floors_block_is_installed_as_given()
    {
        // A floor moved down still takes effect.
        var seq = new FloorSequence(maxLo: 2);
        seq.ResetDatabaseTo(50);
        seq.NextLong().ShouldBe(101);

        await seq.SetFloor(10);

        seq.CurrentHi.ShouldBe(6);
        seq.NextLong().ShouldBe(13);
    }

    [Fact]
    public async Task without_the_install_count_the_block_is_still_installed_as_given()
    {
        // TrySetCurrentHi(raw) from outside an advance keeps the behaviour it always had.
        var seq = new FloorSequence(maxLo: 2) { UseInstallCount = false };
        seq.NextLong().ShouldBe(1);
        seq.NextLong().ShouldBe(2);
        seq.BetweenReserveAndInstall = () => seq.NextLong().ShouldBe(1005);

        await seq.SetFloor(1000);

        seq.CurrentHi.ShouldBe(501);
        seq.NextLong().ShouldBe(1003);
    }

    [Fact]
    public async Task the_install_count_counts_installs_and_not_dropped_blocks()
    {
        var seq = new FloorSequence(maxLo: 2);
        seq.Installs.ShouldBe(0);

        seq.NextLong();
        seq.Installs.ShouldBe(1);

        seq.BetweenReserveAndInstall = () => seq.NextLong();
        seq.NextLong(); // exhaust hi 0, so the hook's NextLong advances
        await seq.SetFloor(1000);

        // hi 0, the hook's hi 502; the floor's hi 501 was dropped.
        seq.Installs.ShouldBe(2);
    }

    [Fact]
    public void a_negative_install_count_is_refused()
    {
        var seq = new NegativeCount();
        Should.Throw<ArgumentOutOfRangeException>(() => seq.Install(3, -1));
    }

    private sealed class NegativeCount(): HiloSequenceBase("things", new HiloSettings { MaxLo = 2 })
    {
        public bool Install(long hi, long before) => TrySetCurrentHi(hi, before);
        public override Task AdvanceToNextHi(CancellationToken ct = default) => Task.CompletedTask;
        protected override void AdvanceToNextHiSync() { }
        public override Task SetFloor(long floor) => Task.CompletedTask;
    }
}
