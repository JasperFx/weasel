using System.Globalization;

namespace Weasel.Core.Partitioning;

/// <summary>
///     One partition of a rolling time window: the half-open interval
///     <c>[From, To)</c> and the table-name suffix that identifies it.
/// </summary>
/// <param name="Suffix">Partition table name suffix, e.g. <c>m202607</c>.</param>
/// <param name="From">Inclusive lower bound of the partition, always UTC.</param>
/// <param name="To">Exclusive upper bound of the partition, always UTC.</param>
public sealed record TimeWindowPartition(string Suffix, DateTimeOffset From, DateTimeOffset To);

/// <summary>
///     Declarative description of a rolling time-window partitioning scheme: a period size, how many
///     periods to provision <em>ahead</em> of now, and how many periods to retain <em>behind</em> now
///     before a partition is aged out.
///     <para>
///         The whole point of the type is that the partition set is a pure function of the policy and
///         the current time. Nothing about it is stored, so "now moved forward one month" produces a
///         window that differs from the database's by exactly one <em>new</em> partition at the leading
///         edge and one <em>aged</em> partition at the trailing edge — an additive create plus a
///         retention drop, never a rebuild.
///     </para>
///     <para>
///         All arithmetic is done in UTC. See JasperFx/weasel#401.
///     </para>
/// </summary>
public class RollingWindowPolicy
{
    /// <summary>
    ///     Create a rolling window policy.
    /// </summary>
    /// <param name="period">The size of a single partition.</param>
    /// <param name="periodsAhead">
    ///     How many periods beyond the current one to provision. Must be zero or greater, but at least
    ///     one is strongly recommended so that rows written at the very end of a period always have a
    ///     partition waiting for them.
    /// </param>
    /// <param name="periodsBehind">
    ///     How many completed periods to retain. Partitions older than this are aged out. Zero means
    ///     "keep only the current period and anything provisioned ahead of it".
    /// </param>
    public RollingWindowPolicy(PartitionPeriod period, int periodsAhead, int periodsBehind)
    {
        if (periodsAhead < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(periodsAhead), periodsAhead,
                "The number of periods to provision ahead cannot be negative");
        }

        if (periodsBehind < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(periodsBehind), periodsBehind,
                "The number of periods to retain behind cannot be negative");
        }

        Period = period;
        PeriodsAhead = periodsAhead;
        PeriodsBehind = periodsBehind;
    }

    /// <summary>
    ///     The size of a single partition.
    /// </summary>
    public PartitionPeriod Period { get; }

    /// <summary>
    ///     How many periods beyond the current one are provisioned.
    /// </summary>
    public int PeriodsAhead { get; }

    /// <summary>
    ///     How many completed periods are retained before a partition is aged out.
    /// </summary>
    public int PeriodsBehind { get; }

    /// <summary>
    ///     The day a <see cref="PartitionPeriod.Week" /> partition begins on. Defaults to Monday, and is
    ///     ignored by every other period. Changing this on a database that already holds weekly partitions
    ///     re-bases every boundary, so treat it as a create-time decision.
    /// </summary>
    public DayOfWeek FirstDayOfWeek { get; init; } = DayOfWeek.Monday;

    /// <summary>
    ///     Hourly partitions: one partition per clock hour.
    /// </summary>
    public static RollingWindowPolicy Hourly(int periodsAhead, int periodsBehind)
        => new(PartitionPeriod.Hour, periodsAhead, periodsBehind);

    /// <summary>
    ///     Daily partitions: one partition per calendar day.
    /// </summary>
    public static RollingWindowPolicy Daily(int periodsAhead, int periodsBehind)
        => new(PartitionPeriod.Day, periodsAhead, periodsBehind);

    /// <summary>
    ///     Weekly partitions: one partition per week, beginning on <see cref="FirstDayOfWeek" />.
    /// </summary>
    public static RollingWindowPolicy Weekly(int periodsAhead, int periodsBehind)
        => new(PartitionPeriod.Week, periodsAhead, periodsBehind);

    /// <summary>
    ///     Monthly partitions: one partition per calendar month.
    /// </summary>
    public static RollingWindowPolicy Monthly(int periodsAhead, int periodsBehind)
        => new(PartitionPeriod.Month, periodsAhead, periodsBehind);

    /// <summary>
    ///     Yearly partitions: one partition per calendar year.
    /// </summary>
    public static RollingWindowPolicy Yearly(int periodsAhead, int periodsBehind)
        => new(PartitionPeriod.Year, periodsAhead, periodsBehind);

    /// <summary>
    ///     The UTC instant at which the period containing <paramref name="moment" /> begins.
    /// </summary>
    public DateTimeOffset StartOfPeriod(DateTimeOffset moment)
    {
        var utc = moment.ToUniversalTime();

        return Period switch
        {
            PartitionPeriod.Hour => new DateTimeOffset(utc.Year, utc.Month, utc.Day, utc.Hour, 0, 0, TimeSpan.Zero),
            PartitionPeriod.Day => new DateTimeOffset(utc.Year, utc.Month, utc.Day, 0, 0, 0, TimeSpan.Zero),
            PartitionPeriod.Week => startOfWeek(utc),
            PartitionPeriod.Month => new DateTimeOffset(utc.Year, utc.Month, 1, 0, 0, 0, TimeSpan.Zero),
            PartitionPeriod.Year => new DateTimeOffset(utc.Year, 1, 1, 0, 0, 0, TimeSpan.Zero),
            _ => throw new ArgumentOutOfRangeException(nameof(moment), Period, "Unsupported partition period")
        };
    }

    /// <summary>
    ///     Shift a period start forward (or backward, for a negative count) by whole periods.
    /// </summary>
    public DateTimeOffset AddPeriods(DateTimeOffset periodStart, int periods)
    {
        return Period switch
        {
            PartitionPeriod.Hour => periodStart.AddHours(periods),
            PartitionPeriod.Day => periodStart.AddDays(periods),
            PartitionPeriod.Week => periodStart.AddDays(7 * periods),
            PartitionPeriod.Month => periodStart.AddMonths(periods),
            PartitionPeriod.Year => periodStart.AddYears(periods),
            _ => throw new ArgumentOutOfRangeException(nameof(periodStart), Period, "Unsupported partition period")
        };
    }

    /// <summary>
    ///     The oldest period start this policy still retains at <paramref name="now" />. A partition whose
    ///     period begins strictly before this instant is aged out.
    /// </summary>
    public DateTimeOffset RetentionFloor(DateTimeOffset now)
        => AddPeriods(StartOfPeriod(now), -PeriodsBehind);

    /// <summary>
    ///     The full set of partitions this policy expects to exist at <paramref name="now" />, ordered
    ///     oldest first: <see cref="PeriodsBehind" /> retained periods, the current period, then
    ///     <see cref="PeriodsAhead" /> provisioned ahead.
    /// </summary>
    public IReadOnlyList<TimeWindowPartition> Window(DateTimeOffset now)
    {
        var current = StartOfPeriod(now);
        var window = new List<TimeWindowPartition>(PeriodsBehind + PeriodsAhead + 1);

        for (var i = -PeriodsBehind; i <= PeriodsAhead; i++)
        {
            var from = AddPeriods(current, i);
            var to = AddPeriods(current, i + 1);
            window.Add(new TimeWindowPartition(SuffixFor(from), from, to));
        }

        return window;
    }

    /// <summary>
    ///     The partition table name suffix for the period containing <paramref name="moment" />. The
    ///     encoding is a single period-type letter followed by the fixed-width, zero-padded period start:
    ///     <c>y2026</c>, <c>m202607</c>, <c>w20260727</c>, <c>d20260730</c>, <c>h2026073014</c>. It is
    ///     lexically sortable, unambiguous across period types, and round-trips through
    ///     <see cref="TryParseSuffix" />.
    /// </summary>
    public string SuffixFor(DateTimeOffset moment)
    {
        var start = StartOfPeriod(moment);

        return Period switch
        {
            PartitionPeriod.Hour => "h" + start.ToString("yyyyMMddHH", CultureInfo.InvariantCulture),
            PartitionPeriod.Day => "d" + start.ToString("yyyyMMdd", CultureInfo.InvariantCulture),
            PartitionPeriod.Week => "w" + start.ToString("yyyyMMdd", CultureInfo.InvariantCulture),
            PartitionPeriod.Month => "m" + start.ToString("yyyyMM", CultureInfo.InvariantCulture),
            PartitionPeriod.Year => "y" + start.ToString("yyyy", CultureInfo.InvariantCulture),
            _ => throw new ArgumentOutOfRangeException(nameof(moment), Period, "Unsupported partition period")
        };
    }

    /// <summary>
    ///     Try to read a partition suffix back into the period start it encodes. Returns false for any
    ///     suffix this policy did not produce — a different period size, a hand-created partition, or a
    ///     date that is not actually a period start for this policy. Callers use that to leave partitions
    ///     they do not own strictly alone.
    /// </summary>
    public bool TryParseSuffix(string? suffix, out DateTimeOffset periodStart)
    {
        periodStart = default;

        if (string.IsNullOrWhiteSpace(suffix))
        {
            return false;
        }

        var value = suffix.Trim();

        var (prefix, digitCount) = Period switch
        {
            PartitionPeriod.Hour => ('h', 10),
            PartitionPeriod.Day => ('d', 8),
            PartitionPeriod.Week => ('w', 8),
            PartitionPeriod.Month => ('m', 6),
            PartitionPeriod.Year => ('y', 4),
            _ => throw new ArgumentOutOfRangeException(nameof(suffix), Period, "Unsupported partition period")
        };

        if (value.Length != digitCount + 1 || char.ToLowerInvariant(value[0]) != prefix)
        {
            return false;
        }

        var digits = value[1..];

        // Pad the partial dates out to a full date so the parse never falls back on "today" for the
        // components the suffix does not carry.
        var (text, format) = Period switch
        {
            PartitionPeriod.Hour => (digits, "yyyyMMddHH"),
            PartitionPeriod.Year => (digits + "0101", "yyyyMMdd"),
            PartitionPeriod.Month => (digits + "01", "yyyyMMdd"),
            _ => (digits, "yyyyMMdd")
        };

        if (!DateTime.TryParseExact(text, format, CultureInfo.InvariantCulture,
                DateTimeStyles.None | DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out var parsed))
        {
            return false;
        }

        var candidate = new DateTimeOffset(parsed, TimeSpan.Zero);

        // A weekly suffix that does not land on FirstDayOfWeek was not produced by this policy.
        if (candidate != StartOfPeriod(candidate))
        {
            return false;
        }

        periodStart = candidate;
        return true;
    }

    private DateTimeOffset startOfWeek(DateTimeOffset utc)
    {
        var day = new DateTimeOffset(utc.Year, utc.Month, utc.Day, 0, 0, 0, TimeSpan.Zero);
        var delta = ((int)day.DayOfWeek - (int)FirstDayOfWeek + 7) % 7;

        return day.AddDays(-delta);
    }

    /// <inheritdoc />
    public override string ToString()
        => $"{Period}, {PeriodsAhead} ahead, {PeriodsBehind} retained";
}
