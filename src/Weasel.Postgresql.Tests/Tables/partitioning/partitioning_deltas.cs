using System.Collections;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

public class partitioning_deltas
{
    private readonly HashPartitioning hash1 =
        new HashPartitioning { Columns = ["role"], Suffixes = ["one", "two", "three"] };

    private readonly RangePartitioning range1 = new RangePartitioning { Columns = ["roles"] }
        .AddRange("one", "a", "b")
        .AddRange("two", "c", "d");

    private readonly ListPartitioning list1 = new ListPartitioning { Columns = ["roles"] }
        .AddPartition("one", "one")
        .AddPartition("two", "two");

    [Fact]
    public void always_return_a_full_change_when_partioning_is_different()
    {
        var table = new Table(new DbObjectName("partitions", "people"));

        IPartition[] partitions = default;
        hash1.CreateDelta(table, list1, out partitions).ShouldBe(PartitionDelta.Rebuild);
        hash1.CreateDelta(table, range1, out partitions).ShouldBe(PartitionDelta.Rebuild);
        list1.CreateDelta(table, range1, out partitions).ShouldBe(PartitionDelta.Rebuild);
        list1.CreateDelta(table, hash1, out partitions).ShouldBe(PartitionDelta.Rebuild);
        hash1.CreateDelta(table, list1, out partitions).ShouldBe(PartitionDelta.Rebuild);
        hash1.CreateDelta(table, range1, out partitions).ShouldBe(PartitionDelta.Rebuild);
    }

    [Fact]
    public void hash_is_different_with_different_columns()
    {
        var table = new Table(new DbObjectName("partitions", "people"));

        var hash2 =
            new HashPartitioning { Columns = ["role", "other"], Suffixes = ["one", "two", "three"] };

        IPartition[] partitions = default;
        hash2.CreateDelta(table, hash1, out partitions).ShouldBe(PartitionDelta.Rebuild);
        hash1.CreateDelta(table, hash2, out partitions).ShouldBe(PartitionDelta.Rebuild);
    }

    [Fact]
    public void hash_is_automatic_rebuild_if_anything_is_different()
    {
        IPartition[] partitions = default;
        var table = new Table(new DbObjectName("partitions", "people"));

        var hash2 =
            new HashPartitioning { Columns = ["role"], Suffixes = ["four", "two", "three"] };

        var hash3 =
            new HashPartitioning { Columns = ["role"], Suffixes = ["one", "two", "three", "four"] };

        hash2.CreateDelta(table, hash1, out partitions).ShouldBe(PartitionDelta.Rebuild);
        hash1.CreateDelta(table, hash2, out partitions).ShouldBe(PartitionDelta.Rebuild);
        hash3.CreateDelta(table, hash2, out partitions).ShouldBe(PartitionDelta.Rebuild);
        hash3.CreateDelta(table, hash1, out partitions).ShouldBe(PartitionDelta.Rebuild);
        hash1.CreateDelta(table, hash2, out partitions).ShouldBe(PartitionDelta.Rebuild);
        hash1.CreateDelta(table, hash3, out partitions).ShouldBe(PartitionDelta.Rebuild);


    }

    [Fact]
    public void range_is_different_with_different_columns()
    {
        var table = new Table(new DbObjectName("partitions", "people"));
        var range2 = new RangePartitioning { Columns = ["roles", "other"] }
            .AddRange("one", "a", "b")
            .AddRange("two", "c", "d");

        IPartition[] partitions = default;

        range2.CreateDelta(table, range1, out partitions).ShouldBe(PartitionDelta.Rebuild);
        range1.CreateDelta(table, range2, out partitions).ShouldBe(PartitionDelta.Rebuild);
    }

    [Fact]
    public void range_is_different_with_non_matching_partition_because_range_is_different()
    {
        var table = new Table(new DbObjectName("partitions", "people"));
        var range2 = new RangePartitioning { Columns = ["roles",] }
            .AddRange("one", "a", "b")
            .AddRange("two", "f", "g");

        IPartition[] partitions = default;

        range2.CreateDelta(table, range1, out partitions).ShouldBe(PartitionDelta.Rebuild);
        range1.CreateDelta(table, range2, out partitions).ShouldBe(PartitionDelta.Rebuild);
    }

    [Fact]
    public void range_is_different_with_non_matching_partition_because_name_is_different()
    {
        var table = new Table(new DbObjectName("partitions", "people"));
        var range2 = new RangePartitioning { Columns = ["roles",] }
            .AddRange("one", "a", "b")
            .AddRange("two_different", "c", "d");

        IPartition[] partitions = default;

        range2.CreateDelta(table, range1, out partitions).ShouldBe(PartitionDelta.Rebuild);
        range1.CreateDelta(table, range2, out partitions).ShouldBe(PartitionDelta.Rebuild);
    }

    [Fact]
    public void range_is_additive_with_all_new_partitions()
    {
        var range2 = new RangePartitioning { Columns = ["roles"] }
            .AddRange("one", "a", "b")
            .AddRange("two", "c", "d")
            .AddRange("three", "e", "f");

        var table = new Table(new DbObjectName("partitions", "people"));

        range2.CreateDelta(table, range1, out var missing).ShouldBe(PartitionDelta.Additive);
        missing.Single().ShouldBe(new RangePartition("three", "'e'", "'f'"));
    }

    [Fact]
    public void range_rebuild_if_partition_has_been_removed()
    {
        var range2 = new RangePartitioning { Columns = ["roles"] }
            .AddRange("one", "a", "b")
            .AddRange("two", "c", "d")
            .AddRange("three", "e", "f");

        var table = new Table(new DbObjectName("partitions", "people"));

        range1.CreateDelta(table, range2, out var missing).ShouldBe(PartitionDelta.Rebuild);
    }


    [Fact]
    public void list_is_different_with_non_matching_partition_because_name_is_different()
    {
        var table = new Table(new DbObjectName("partitions", "people"));
        var list2 = new ListPartitioning { Columns = ["roles",] }
            .AddPartition("one", "one")
            .AddPartition("different", "two");

        IPartition[] partitions = default;

        list2.CreateDelta(table, list1, out partitions).ShouldBe(PartitionDelta.Rebuild);
        list1.CreateDelta(table, list2, out partitions).ShouldBe(PartitionDelta.Rebuild);
    }

    [Fact]
    public void list_is_additive_with_all_new_partitions()
    {
        var list2 = new ListPartitioning { Columns = ["roles"] }
            .AddPartition("one", "one")
            .AddPartition("two", "two")
            .AddPartition("three", "three");

        var table = new Table(new DbObjectName("partitions", "people"));

        list2.CreateDelta(table, list1, out var missing).ShouldBe(PartitionDelta.Additive);
        missing.Single().ShouldBe(new ListPartition("three", "'three'"));
    }

    [Fact]
    public void list_rebuild_if_partition_has_been_removed()
    {
        var list2 = new ListPartitioning { Columns = ["roles"] }
            .AddPartition("one", "one")
            .AddPartition("two", "two")
            .AddPartition("three", "three");

        var table = new Table(new DbObjectName("partitions", "people"));

        list1.CreateDelta(table, list2, out var missing).ShouldBe(PartitionDelta.Rebuild);
    }

    [Fact]
    public void list_is_different_with_different_columns()
    {
        var table = new Table(new DbObjectName("partitions", "people"));

        var list2 = new ListPartitioning { Columns = ["roles", "other"] }
            .AddPartition("one", "one")
            .AddPartition("two", "two");

        IPartition[] partitions = default;

        list2.CreateDelta(table, list1, out partitions).ShouldBe(PartitionDelta.Rebuild);
        list1.CreateDelta(table, list2, out partitions).ShouldBe(PartitionDelta.Rebuild);
    }

    [Fact]
    public void return_none_if_equal()
    {
        var table = new Table(new DbObjectName("partitions", "people"));

        IPartition[] partitions = default;
        hash1.CreateDelta(table, hash1, out partitions).ShouldBe(PartitionDelta.None);
        list1.CreateDelta(table, list1, out partitions).ShouldBe(PartitionDelta.None);
        range1.CreateDelta(table, range1, out partitions).ShouldBe(PartitionDelta.None);
    }


}
