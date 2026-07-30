using Shouldly;
using Weasel.Core.Partitioning;
using Xunit;

namespace Weasel.Core.Tests.Partitioning;

public class RollingWindowPolicyTests
{
    private static readonly DateTimeOffset TheMoment =
        new(2026, 7, 30, 14, 37, 21, TimeSpan.Zero);

    [Fact]
    public void negative_periods_ahead_is_rejected()
    {
        Should.Throw<ArgumentOutOfRangeException>(() => new RollingWindowPolicy(PartitionPeriod.Month, -1, 3));
    }

    [Fact]
    public void negative_periods_behind_is_rejected()
    {
        Should.Throw<ArgumentOutOfRangeException>(() => new RollingWindowPolicy(PartitionPeriod.Month, 3, -1));
    }

    [Theory]
    [InlineData(PartitionPeriod.Hour, "2026-07-30T14:00:00+00:00")]
    [InlineData(PartitionPeriod.Day, "2026-07-30T00:00:00+00:00")]
    [InlineData(PartitionPeriod.Week, "2026-07-27T00:00:00+00:00")]
    [InlineData(PartitionPeriod.Month, "2026-07-01T00:00:00+00:00")]
    [InlineData(PartitionPeriod.Year, "2026-01-01T00:00:00+00:00")]
    public void start_of_period(PartitionPeriod period, string expected)
    {
        new RollingWindowPolicy(period, 1, 1).StartOfPeriod(TheMoment)
            .ShouldBe(DateTimeOffset.Parse(expected));
    }

    [Fact]
    public void start_of_period_converts_to_utc_first()
    {
        // 2026-07-30 01:30 -06:00 is 2026-07-30 07:30 UTC, so the day partition is still the 30th.
        var local = new DateTimeOffset(2026, 7, 30, 1, 30, 0, TimeSpan.FromHours(-6));

        RollingWindowPolicy.Daily(1, 1).StartOfPeriod(local)
            .ShouldBe(new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero));

        // ...but 2026-07-29 20:00 -06:00 is already the 30th in UTC.
        var evening = new DateTimeOffset(2026, 7, 29, 20, 0, 0, TimeSpan.FromHours(-6));

        RollingWindowPolicy.Daily(1, 1).StartOfPeriod(evening)
            .ShouldBe(new DateTimeOffset(2026, 7, 30, 0, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public void week_honors_first_day_of_week()
    {
        // 2026-07-30 is a Thursday.
        var sundayBased = new RollingWindowPolicy(PartitionPeriod.Week, 1, 1) { FirstDayOfWeek = DayOfWeek.Sunday };

        sundayBased.StartOfPeriod(TheMoment)
            .ShouldBe(new DateTimeOffset(2026, 7, 26, 0, 0, 0, TimeSpan.Zero));
    }

    [Theory]
    [InlineData(PartitionPeriod.Hour, "h2026073014")]
    [InlineData(PartitionPeriod.Day, "d20260730")]
    [InlineData(PartitionPeriod.Week, "w20260727")]
    [InlineData(PartitionPeriod.Month, "m202607")]
    [InlineData(PartitionPeriod.Year, "y2026")]
    public void suffix_for(PartitionPeriod period, string expected)
    {
        new RollingWindowPolicy(period, 1, 1).SuffixFor(TheMoment).ShouldBe(expected);
    }

    [Theory]
    [InlineData(PartitionPeriod.Hour)]
    [InlineData(PartitionPeriod.Day)]
    [InlineData(PartitionPeriod.Week)]
    [InlineData(PartitionPeriod.Month)]
    [InlineData(PartitionPeriod.Year)]
    public void suffix_round_trips(PartitionPeriod period)
    {
        var policy = new RollingWindowPolicy(period, 1, 1);
        var suffix = policy.SuffixFor(TheMoment);

        policy.TryParseSuffix(suffix, out var start).ShouldBeTrue();
        start.ShouldBe(policy.StartOfPeriod(TheMoment));
    }

    [Fact]
    public void suffixes_sort_lexically_in_chronological_order()
    {
        var policy = RollingWindowPolicy.Monthly(2, 12);
        var suffixes = policy.Window(TheMoment).Select(x => x.Suffix).ToArray();

        suffixes.OrderBy(x => x, StringComparer.Ordinal).ShouldBe(suffixes);
    }

    [Fact]
    public void will_not_parse_a_suffix_from_another_period_size()
    {
        RollingWindowPolicy.Monthly(1, 1).TryParseSuffix("d20260730", out _).ShouldBeFalse();
        RollingWindowPolicy.Daily(1, 1).TryParseSuffix("m202607", out _).ShouldBeFalse();
    }

    [Fact]
    public void will_not_parse_a_foreign_suffix()
    {
        var policy = RollingWindowPolicy.Monthly(1, 1);

        policy.TryParseSuffix("default", out _).ShouldBeFalse();
        policy.TryParseSuffix("twenties", out _).ShouldBeFalse();
        policy.TryParseSuffix(null, out _).ShouldBeFalse();
        policy.TryParseSuffix("", out _).ShouldBeFalse();
        policy.TryParseSuffix("m20260x", out _).ShouldBeFalse();
        policy.TryParseSuffix("m2026071", out _).ShouldBeFalse();
    }

    [Fact]
    public void will_not_parse_a_week_suffix_that_is_not_on_the_first_day_of_week()
    {
        var policy = RollingWindowPolicy.Weekly(1, 1);

        // Monday-based weeks: 2026-07-27 is a Monday, 2026-07-28 is not.
        policy.TryParseSuffix("w20260727", out _).ShouldBeTrue();
        policy.TryParseSuffix("w20260728", out _).ShouldBeFalse();
    }

    [Fact]
    public void window_covers_retained_current_and_provisioned_periods()
    {
        var policy = RollingWindowPolicy.Monthly(periodsAhead: 2, periodsBehind: 3);

        var window = policy.Window(TheMoment);

        window.Count.ShouldBe(6);
        window.Select(x => x.Suffix)
            .ShouldBe(["m202604", "m202605", "m202606", "m202607", "m202608", "m202609"]);
    }

    [Fact]
    public void window_partitions_are_contiguous_and_half_open()
    {
        var window = RollingWindowPolicy.Monthly(2, 2).Window(TheMoment);

        for (var i = 1; i < window.Count; i++)
        {
            window[i].From.ShouldBe(window[i - 1].To);
        }

        window[0].From.ShouldBe(new DateTimeOffset(2026, 5, 1, 0, 0, 0, TimeSpan.Zero));
        window[^1].To.ShouldBe(new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public void a_zero_retention_window_still_has_the_current_period()
    {
        var window = RollingWindowPolicy.Daily(periodsAhead: 0, periodsBehind: 0).Window(TheMoment);

        window.Count.ShouldBe(1);
        window[0].Suffix.ShouldBe("d20260730");
    }

    [Fact]
    public void retention_floor_is_the_oldest_period_in_the_window()
    {
        var policy = RollingWindowPolicy.Monthly(2, 3);

        policy.RetentionFloor(TheMoment).ShouldBe(policy.Window(TheMoment)[0].From);
    }

    [Fact]
    public void rolling_the_clock_forward_one_period_adds_one_and_ages_one()
    {
        var policy = RollingWindowPolicy.Monthly(periodsAhead: 1, periodsBehind: 2);

        var before = policy.Window(TheMoment).Select(x => x.Suffix).ToArray();
        var after = policy.Window(TheMoment.AddMonths(1)).Select(x => x.Suffix).ToArray();

        // This is the whole premise of #401: a window that has rolled forward differs from the database's
        // by exactly one new partition and one aged one — never a wholesale change that reads as drift.
        after.Except(before).ShouldHaveSingleItem().ShouldBe("m202609");
        before.Except(after).ShouldHaveSingleItem().ShouldBe("m202605");
    }

    [Fact]
    public void month_arithmetic_survives_short_months()
    {
        var policy = RollingWindowPolicy.Monthly(1, 1);
        var lastDayOfJanuary = new DateTimeOffset(2026, 1, 31, 23, 59, 59, TimeSpan.Zero);

        policy.Window(lastDayOfJanuary).Select(x => x.Suffix)
            .ShouldBe(["m202512", "m202601", "m202602"]);
    }
}
