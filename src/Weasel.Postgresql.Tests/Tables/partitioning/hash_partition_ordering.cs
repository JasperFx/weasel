using System.Reflection;
using Shouldly;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

/// <summary>
///     pg_inherits has no inherent order and the read query imposes none, so the partitions of a
///     hash-partitioned table can come back in any order.
/// </summary>
public class hash_partition_ordering
{
    // The public API assigns remainders by position, so it cannot express "the same partitions in a
    // different order" -- which is exactly what the catalog hands back. Reordering the backing list
    // is the only way to reproduce a read whose rows arrived in another order.
    private static HashPartitioning asReadInOrder(params string[] catalogOrder)
    {
        var partitioning = new HashPartitioning { Columns = ["last_name"], Suffixes = ["one", "two", "three"] };

        var field = typeof(HashPartitioning)
            .GetField("_partitions", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var partitions = (List<HashPartition>)field.GetValue(partitioning)!;

        var reordered = catalogOrder.Select(s => partitions.Single(p => p.Suffix == s)).ToList();
        partitions.Clear();
        partitions.AddRange(reordered);

        return partitioning;
    }

    [Theory]
    [InlineData("one", "two", "three")]
    [InlineData("three", "one", "two")]
    [InlineData("two", "three", "one")]
    [InlineData("three", "two", "one")]
    public void the_same_partitions_in_any_order_are_not_a_rebuild(params string[] catalogOrder)
    {
        var expected = new HashPartitioning { Columns = ["last_name"], Suffixes = ["one", "two", "three"] };

        expected.CreateDelta(new Table("partitions.people"), asReadInOrder(catalogOrder), out _)
            .ShouldBe(PartitionDelta.None);
    }

    [Fact]
    public void swapping_which_suffix_holds_which_remainder_is_still_a_rebuild()
    {
        var expected = new HashPartitioning { Columns = ["last_name"], Suffixes = ["one", "two", "three"] };
        var actual = new HashPartitioning { Columns = ["last_name"], Suffixes = ["two", "one", "three"] };

        expected.CreateDelta(new Table("partitions.people"), actual, out _)
            .ShouldBe(PartitionDelta.Rebuild);
    }
}
