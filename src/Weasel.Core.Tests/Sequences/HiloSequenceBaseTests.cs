using Shouldly;
using Weasel.Core.Sequences;
using Xunit;

namespace Weasel.Core.Tests.Sequences;

public class HiloSequenceBaseTests
{
    /// <summary>
    ///     Test double that supplies the "database I/O" the abstract base leaves
    ///     open. Each "advance to next hi" just bumps an in-memory hi counter,
    ///     matching what a real dialect does after a successful fetch.
    /// </summary>
    private class FakeHiloSequence: HiloSequenceBase
    {
        private long _nextHi;

        public FakeHiloSequence(string entityName, IReadOnlyHiloSettings settings)
            : base(entityName, settings)
        {
        }

        public int AdvanceCount { get; private set; }

        // Expose the protected hi-setter so a test can drive the "database
        // returned a negative value, could not secure the next hi" branch.
        public bool TrySetCurrentHiForTest(object? raw) => TrySetCurrentHi(raw);

        public override Task AdvanceToNextHi(CancellationToken ct = default)
        {
            DoAdvance();
            return Task.CompletedTask;
        }

        protected override void AdvanceToNextHiSync() => DoAdvance();

        public override Task SetFloor(long floor)
        {
            // Mirror the dialect contract: ensure the next issued id clears the floor.
            var pages = (long)Math.Ceiling((double)floor / MaxLo);
            _nextHi = pages;
            DoAdvance();
            return Task.CompletedTask;
        }

        private void DoAdvance()
        {
            AdvanceCount++;
            TrySetCurrentHi(_nextHi);
            _nextHi++;
        }
    }

    private static FakeHiloSequence Sequence(int maxLo = 3)
        => new("things", new HiloSettings { MaxLo = maxLo });

    [Fact]
    public void initial_state_requires_a_hi_advance()
    {
        var seq = Sequence();
        seq.CurrentHi.ShouldBe(-1);
        seq.CurrentLo.ShouldBe(1);
        seq.ShouldAdvanceHi().ShouldBeTrue();
    }

    [Fact]
    public void next_long_issues_a_contiguous_run_within_a_single_hi()
    {
        var seq = Sequence(maxLo: 3);

        // hi 0 -> ids 1,2,3   (CurrentHi*MaxLo + CurrentLo, lo running 1..MaxLo)
        seq.NextLong().ShouldBe(1);
        seq.NextLong().ShouldBe(2);
        seq.NextLong().ShouldBe(3);

        // Only one database advance was needed for the whole lo range.
        seq.AdvanceCount.ShouldBe(1);
    }

    [Fact]
    public void next_long_advances_hi_when_lo_range_is_exhausted()
    {
        var seq = Sequence(maxLo: 3);

        seq.NextLong().ShouldBe(1);
        seq.NextLong().ShouldBe(2);
        seq.NextLong().ShouldBe(3);

        // hi advances to 1 -> next id is (1*3)+1 = 4
        seq.NextLong().ShouldBe(4);
        seq.AdvanceCount.ShouldBe(2);
    }

    [Fact]
    public void next_int_is_a_narrowing_cast_of_next_long()
    {
        var seq = Sequence(maxLo: 1000);
        seq.NextInt().ShouldBe(1);
        seq.NextInt().ShouldBe(2);
    }

    [Fact]
    public void try_set_current_hi_accepts_non_negative_and_resets_lo()
    {
        var seq = Sequence(maxLo: 3);
        seq.NextLong(); // advance lo off its initial value

        seq.TrySetCurrentHiForTest(7L).ShouldBeTrue();
        seq.CurrentHi.ShouldBe(7);
        seq.CurrentLo.ShouldBe(1); // reset on a successful hi fetch
    }

    [Fact]
    public void try_set_current_hi_rejects_negative_without_resetting_lo()
    {
        var seq = Sequence(maxLo: 3);
        seq.NextLong();
        seq.NextLong();
        var loBefore = seq.CurrentLo;

        // A negative scalar means the dialect couldn't secure the next hi — the
        // caller should retry, so lo must be left untouched.
        seq.TrySetCurrentHiForTest(-1L).ShouldBeFalse();
        seq.CurrentLo.ShouldBe(loBefore);
    }

    [Fact]
    public async Task set_floor_pushes_ids_past_the_floor()
    {
        var seq = Sequence(maxLo: 10);

        await seq.SetFloor(95);

        // 95 / 10 -> 10 pages, so the next hi is 10 -> first id (10*10)+1 = 101 > 95.
        seq.NextLong().ShouldBeGreaterThan(95);
    }
}
