namespace Weasel.Core.Partitioning;

/// <summary>
///     The size of a single partition in a rolling time-window partitioning scheme.
/// </summary>
public enum PartitionPeriod
{
    /// <summary>
    ///     One partition per clock hour, starting at the top of the hour (UTC).
    /// </summary>
    Hour,

    /// <summary>
    ///     One partition per calendar day, starting at midnight (UTC).
    /// </summary>
    Day,

    /// <summary>
    ///     One partition per week, starting at midnight (UTC) on
    ///     <see cref="RollingWindowPolicy.FirstDayOfWeek" />.
    /// </summary>
    Week,

    /// <summary>
    ///     One partition per calendar month, starting on the first of the month (UTC).
    /// </summary>
    Month,

    /// <summary>
    ///     One partition per calendar year, starting on January 1st (UTC).
    /// </summary>
    Year
}
