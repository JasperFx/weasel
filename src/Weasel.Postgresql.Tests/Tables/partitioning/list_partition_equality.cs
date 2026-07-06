using Shouldly;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

public class list_partition_equality
{
    [Fact]
    public void integer_value_matches_quoted_readback()
    {
        // declared unquoted (20) vs PostgreSQL read-back single-quoted ('20') — weasel#320
        var declared = new ListPartition("twenties", "20");
        var readBack = new ListPartition("twenties", "'20'");

        declared.Equals(readBack).ShouldBeTrue();
    }

    [Fact]
    public void date_value_matches_quoted_readback_across_time_zones()
    {
        // Same instant, rendered by PostgreSQL in a non-UTC session time zone
        var declared = new ListPartition("start", "'2026-01-01 00:00:00+00'");
        var readBack = new ListPartition("start", "'2025-12-31 18:00:00-06'");

        declared.Equals(readBack).ShouldBeTrue();
    }

    [Fact]
    public void not_equal_when_a_value_differs()
    {
        new ListPartition("twenties", "20")
            .Equals(new ListPartition("twenties", "21"))
            .ShouldBeFalse();
    }

    [Fact]
    public void not_equal_when_suffix_differs()
    {
        new ListPartition("twenties", "20")
            .Equals(new ListPartition("thirties", "20"))
            .ShouldBeFalse();
    }

    [Fact]
    public void get_hashcode_is_consistent_with_equals_across_quoting()
    {
        var declared = new ListPartition("twenties", "20");
        var readBack = new ListPartition("twenties", "'20'");

        declared.GetHashCode().ShouldBe(readBack.GetHashCode());
    }

    [Fact]
    public void get_hashcode_hashes_element_values_not_the_array_reference()
    {
        // Old bug: HashCode.Combine(Values, Suffix) hashed the array reference, so two instances with
        // identical contents hashed differently. The hash must be based on the element values.
        var one = new ListPartition("admin", "'admin'", "'super'");
        var two = new ListPartition("admin", "'admin'", "'super'");

        one.GetHashCode().ShouldBe(two.GetHashCode());
    }
}
