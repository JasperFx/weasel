using Shouldly;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

/// <summary>
///     weasel#524. A partition manager owns the whole partition set, so a statically declared
///     partition alongside one is silently ignored — the caller wrote something that cannot do what
///     they meant.
/// </summary>
/// <remarks>
///     <para>
///     <c>RangePartitioning.AddRange</c> already refused this. <c>ListPartitioning.AddPartition</c>
///     did not, and neither class guarded <c>UsePartitionManager</c> — so even where the refusal
///     existed it depended on the order the fluent calls happened to be written in, which a fluent
///     builder gives no reason to prefer. All four combinations are pinned here, including the one
///     that already threw.
///     </para>
///     <para>
///     This is a refusal, not a correctness fix. Since weasel#520 every call site resolves through
///     the manager first, so a stray statically declared partition is inert rather than wrong.
///     </para>
/// </remarks>
public class partition_manager_guards
{
    private sealed class StubListManager: IListPartitionManager
    {
        public IEnumerable<ListPartition> Partitions() => [new ListPartition("one", "'one'")];
    }

    private sealed class StubRangeManager: IRangePartitionManager
    {
        public IEnumerable<RangePartition> Partitions() => [new RangePartition("y2026", "'2026-01-01'", "'2027-01-01'")];
    }

    // ---- LIST -------------------------------------------------------------

    [Fact]
    public void list_manager_first_then_a_static_partition_is_refused()
    {
        var partitioning = new ListPartitioning { Columns = ["tenant_id"] }
            .UsePartitionManager(new StubListManager());

        Should.Throw<InvalidOperationException>(() => partitioning.AddPartition("two", "two"))
            .Message.ShouldContain("owned by a partition manager");
    }

    [Fact]
    public void list_static_partition_first_then_a_manager_is_refused()
    {
        var partitioning = new ListPartitioning { Columns = ["tenant_id"] }
            .AddPartition("two", "two");

        Should.Throw<InvalidOperationException>(() => partitioning.UsePartitionManager(new StubListManager()))
            .Message.ShouldContain("two");
    }

    // ---- RANGE ------------------------------------------------------------

    [Fact]
    public void range_manager_first_then_a_static_range_is_refused()
    {
        var partitioning = new RangePartitioning { Columns = ["occurred_at"] }
            .UsePartitionManager(new StubRangeManager());

        Should.Throw<InvalidOperationException>(() => partitioning.AddRange("y2027", "2027-01-01", "2028-01-01"))
            .Message.ShouldContain("owned by a partition manager");
    }

    [Fact]
    public void range_static_range_first_then_a_manager_is_refused()
    {
        var partitioning = new RangePartitioning { Columns = ["occurred_at"] }
            .AddRange("y2027", "2027-01-01", "2028-01-01");

        Should.Throw<InvalidOperationException>(() => partitioning.UsePartitionManager(new StubRangeManager()))
            .Message.ShouldContain("y2027");
    }

    // ---- what must still work ---------------------------------------------

    /// <summary>
    ///     The shape every caller in Weasel, its docs and Marten actually uses: a fresh partitioning
    ///     with a manager attached and nothing declared statically.
    /// </summary>
    [Fact]
    public void a_manager_on_a_fresh_partitioning_is_fine()
    {
        Should.NotThrow(() => new ListPartitioning { Columns = ["tenant_id"] }
            .UsePartitionManager(new StubListManager()));

        Should.NotThrow(() => new RangePartitioning { Columns = ["occurred_at"] }
            .UsePartitionManager(new StubRangeManager()));
    }

    [Fact]
    public void several_static_partitions_without_a_manager_are_fine()
    {
        Should.NotThrow(() => new ListPartitioning { Columns = ["tenant_id"] }
            .AddPartition("one", "one")
            .AddPartition("two", "two"));

        Should.NotThrow(() => new RangePartitioning { Columns = ["occurred_at"] }
            .AddRange("y2026", "2026-01-01", "2027-01-01")
            .AddRange("y2027", "2027-01-01", "2028-01-01"));
    }

    /// <summary>
    ///     <c>UsePartitionManager</c> clears the default partition, and that is load-bearing rather
    ///     than incidental: it is what made the empty enumeration in weasel#520 total rather than
    ///     merely short. Adding the guard above must not disturb it.
    /// </summary>
    [Fact]
    public void a_list_manager_still_clears_the_default_partition()
    {
        var partitioning = new ListPartitioning { Columns = ["tenant_id"] };
        partitioning.EnableDefaultPartition.ShouldBeTrue("the default starts enabled");

        partitioning.UsePartitionManager(new StubListManager());

        partitioning.EnableDefaultPartition.ShouldBeFalse();
    }

    /// <summary>
    ///     The internal literal overload is deliberately unguarded — introspection populates a read
    ///     table's partitions, and a table read back out of the catalog never carries a manager. It
    ///     is pinned here so a later tidy-up does not "fix" it for symmetry and break the read path.
    /// </summary>
    [Fact]
    public void the_internal_literal_overload_is_not_guarded()
    {
        var partitioning = new ListPartitioning { Columns = ["tenant_id"] }
            .UsePartitionManager(new StubListManager());

        Should.NotThrow(() => partitioning.AddPartitionWithSqlLiterals("read_back", "'x'"));
    }

    [Fact]
    public void a_null_manager_is_still_an_argument_null_exception()
    {
        Should.Throw<ArgumentNullException>(() =>
            new ListPartitioning { Columns = ["tenant_id"] }.UsePartitionManager(null!));

        Should.Throw<ArgumentNullException>(() =>
            new RangePartitioning { Columns = ["occurred_at"] }.UsePartitionManager(null!));
    }
}
