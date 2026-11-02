using System;
using System.Globalization;
using System.IO;
using System.Threading;
using Shouldly;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables.partitioning;

/// <summary>
///     weasel#764. The partition DDL Weasel generates is SQL, not display text, so it cannot depend on
///     the host's culture — but every arm of <c>RangePartitioning.FormatSqlValue</c> rendered with the
///     current one. Found while fixing polecat#848: Polecat's Storage and Schema suites run 21 of 21 on
///     the default culture and lose 11 to partition tests under <c>--culture th-TH</c>.
/// </summary>
/// <remarks>
///     The cultures below are chosen because each breaks the literal a different way, and all four were
///     measured rather than reasoned about:
///     <list type="bullet">
///         <item><c>th-TH</c> renders a custom date format on the Buddhist calendar, so 2026 becomes 2569.</item>
///         <item><c>fi-FI</c> renders <c>:</c> in <c>HH:mm:ss</c> as <c>.</c> — it is the time-separator
///         placeholder, not a literal colon.</item>
///         <item><c>de-DE</c> gives a decimal a comma separator, which in T-SQL is an argument separator.</item>
///         <item><c>sv-SE</c> writes a negative sign as U+2212 MINUS SIGN rather than ASCII <c>-</c>, which
///         applies to a plain <see cref="int" /> and not only to a decimal.</item>
///     </list>
/// </remarks>
public class partition_literals_are_culture_invariant
{
    public static TheoryData<string> HostileCultures() => new() { "th-TH", "fi-FI", "de-DE", "sv-SE", "lt-LT" };

    private static void InCulture(string name, Action action)
    {
        var original = Thread.CurrentThread.CurrentCulture;
        try
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo(name);
            action();
        }
        finally
        {
            Thread.CurrentThread.CurrentCulture = original;
        }
    }

    [Theory]
    [MemberData(nameof(HostileCultures))]
    public void a_date_boundary_renders_invariantly(string culture)
    {
        InCulture(culture, () =>
            RangePartitioning.FormatSqlValue(new DateTime(2026, 10, 1, 13, 45, 30))
                .ShouldBe("'2026-10-01 13:45:30'"));
    }

    [Theory]
    [MemberData(nameof(HostileCultures))]
    public void a_date_time_offset_boundary_renders_invariantly(string culture)
    {
        InCulture(culture, () =>
            RangePartitioning.FormatSqlValue(new DateTimeOffset(2026, 10, 1, 13, 45, 30, TimeSpan.Zero))
                .ShouldBe("'2026-10-01 13:45:30.0000000 +00:00'"));
    }

    [Theory]
    [MemberData(nameof(HostileCultures))]
    public void numeric_boundaries_render_invariantly(string culture)
    {
        InCulture(culture, () =>
        {
            RangePartitioning.FormatSqlValue(1.5m).ShouldBe("1.5");
            RangePartitioning.FormatSqlValue(-1.5d).ShouldBe("-1.5");
            RangePartitioning.FormatSqlValue(-2.5f).ShouldBe("-2.5");

            // A negative int is enough on its own: sv-SE, fi-FI, lt-LT and nb-NO all write U+2212.
            RangePartitioning.FormatSqlValue(-7).ShouldBe("-7");
            RangePartitioning.FormatSqlValue(-7L).ShouldBe("-7");
        });
    }

    /// <summary>
    ///     The reported symptom, end to end: the <c>SPLIT RANGE</c> the host emits. Under <c>th-TH</c>
    ///     this carried a boundary 543 years from the intended one, and the host's schema disagreed with
    ///     one migrating under an invariant culture.
    /// </summary>
    [Theory]
    [MemberData(nameof(HostileCultures))]
    public void the_generated_split_range_ddl_does_not_depend_on_the_culture(string culture)
    {
        InCulture(culture, () =>
        {
            var table = new Table("partitioning.metrics");
            table.AddColumn<int>("id");
            table.AddColumn("bucket_end", "datetime").NotNull();

            var partitioning = table.PartitionByRange("bucket_end", "datetime");
            partitioning.AddBoundary(new DateTime(2026, 10, 1));

            var writer = new StringWriter();
            partitioning.WriteSplitStatements(writer, table,
                new SqlServerPartitionInfo
                {
                    Column = "bucket_end",
                    SqlDataType = "datetime",
                    IsRangeRight = true,
                    BoundaryValues = new List<string>()
                });

            writer.ToString().ShouldContain("SPLIT RANGE ('2026-10-01 00:00:00')");
            writer.ToString().ShouldNotContain("2569");
        });
    }

    /// <summary>
    ///     The sweep the issue asked for: a column default is rendered into DDL the same way, and a
    ///     negative one picks up the culture's negative sign.
    /// </summary>
    [Theory]
    [MemberData(nameof(HostileCultures))]
    public void a_negative_column_default_renders_invariantly(string culture)
    {
        InCulture(culture, () =>
        {
            var table = new Table("defaults_sample");
            table.AddColumn<int>("ints").DefaultValue(-7);
            table.AddColumn<long>("longs").DefaultValue(-8L);
            table.AddColumn<double>("doubles").DefaultValue(-1.5);

            table.ColumnFor("ints")!.DefaultExpression.ShouldBe("-7");
            table.ColumnFor("longs")!.DefaultExpression.ShouldBe("-8");
            table.ColumnFor("doubles")!.DefaultExpression.ShouldBe("-1.5");
        });
    }
}
