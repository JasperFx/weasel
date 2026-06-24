using System;
using System.Globalization;
using System.Threading;
using Shouldly;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

public class range_partition_value_formatting
{
    [Fact]
    public void format_integer_value_unquoted()
    {
        20.FormatSqlValue().ShouldBe("20");
    }

    [Fact]
    public void format_string_value_quoted()
    {
        "a".FormatSqlValue().ShouldBe("'a'");
    }

    [Fact]
    public void format_date_time_offset_is_canonical_and_invariant()
    {
        var value = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        value.FormatSqlValue().ShouldBe("'2026-01-01 00:00:00+00:00'");
    }

    [Fact]
    public void format_date_time_is_canonical()
    {
        var value = new DateTime(2026, 2, 1, 0, 0, 0);
        value.FormatSqlValue().ShouldBe("'2026-02-01 00:00:00'");
    }

    [Fact]
    public void format_date_time_offset_does_not_depend_on_current_culture()
    {
        var original = Thread.CurrentThread.CurrentCulture;
        try
        {
            // A culture whose default DateTimeOffset.ToString() differs wildly from the invariant form
            Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");
            var value = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
            value.FormatSqlValue().ShouldBe("'2026-01-01 00:00:00+00:00'");
        }
        finally
        {
            Thread.CurrentThread.CurrentCulture = original;
        }
    }

    [Fact]
    public void normalize_strips_quotes_for_integers()
    {
        "20".NormalizePartitionValue().ShouldBe("20");
        "'20'".NormalizePartitionValue().ShouldBe("20");
    }

    [Fact]
    public void normalize_strips_quotes_for_text()
    {
        "'a'".NormalizePartitionValue().ShouldBe("a");
    }

    [Fact]
    public void normalize_text_with_leading_dash_is_not_treated_as_a_date()
    {
        // would only be misparsed if LooksLikeTimestamp were too loose
        "'role-a'".NormalizePartitionValue().ShouldBe("role-a");
    }

    [Fact]
    public void normalize_timestamps_to_the_same_instant_regardless_of_rendered_offset()
    {
        // Same instant, rendered by PostgreSQL in different session time zones
        var utc = "'2026-01-01 00:00:00+00'".NormalizePartitionValue();
        var central = "'2025-12-31 18:00:00-06'".NormalizePartitionValue();

        central.ShouldBe(utc);
    }
}
