using Weasel.SqlServer.Tables.Partitioning;
using Xunit;
using Shouldly;

namespace Weasel.SqlServer.Tests.Tables.Partitioning;

/// <summary>
/// weasel#416, SQL Server side. <see cref="RangePartitioning.FormatSqlValue{T}"/>'s catch-all arm interpolated
/// a value into a single-quoted T-SQL literal without doubling an embedded quote, the same class of defect as
/// the PostgreSQL <c>PartitionExtensions.FormatSqlValue</c> sink. Range boundaries are usually dates or
/// integers, which cannot carry a quote, so this arm is the only exposed one — but it is reached by
/// <c>ManagedRangePartitions</c> and by the boundary read-back in <c>Table.FetchExisting</c>.
/// </summary>
public class range_boundary_literal_escaping
{
    [Fact]
    public void doubles_an_embedded_single_quote()
    {
        RangePartitioning.FormatSqlValue("O'Brien").ShouldBe("'O''Brien'");
    }

    [Fact]
    public void a_value_cannot_terminate_the_literal_and_add_statements()
    {
        RangePartitioning.FormatSqlValue("x'); drop table dbo.victim; --")
            .ShouldBe("'x''); drop table dbo.victim; --'");
    }

    [Fact]
    public void still_formats_ordinary_values_unchanged()
    {
        RangePartitioning.FormatSqlValue("tenant-a").ShouldBe("'tenant-a'");
        RangePartitioning.FormatSqlValue(20).ShouldBe("20");
        RangePartitioning.FormatSqlValue(true).ShouldBe("1");
        RangePartitioning.FormatSqlValue((string?)null).ShouldBe("NULL");
    }
}
