using Shouldly;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     <see cref="TableBase{TColumn,TIndex,TForeignKey}.SetPrimaryKeyOrder" /> pins a composite key's
///     column order, and <see cref="TableBase{TColumn,TIndex,TForeignKey}.PrimaryKeyColumns" /> on
///     every provider has to route through <c>ApplyPrimaryKeyOrder</c> for the pin to mean anything.
/// </summary>
/// <remarks>
///     <para>
///     Nothing in the type system forces that routing — a provider can compute its key columns and
///     return them directly, and the pin is then silently ignored. Which is what every provider did
///     before weasel#517: SQL Server and SQLite grew their own copies of this API (weasel#511,
///     weasel#516) and the other three had none.
///     </para>
///     <para>
///     Whether a provider's delta <em>compares</em> the order is deliberately left to that provider,
///     and is not asserted here. PostgreSQL stores its key as an explicit list and so can express
///     order natively — it compares positionally. SQL Server, Oracle, MySQL and SQLite derive the key
///     from flagged columns, where a model cannot express an order it never pinned, so they compare
///     order only when it was. That difference is real, not an oversight.
///     </para>
/// </remarks>
public class primary_key_order_is_honoured_by_every_provider
{
    /// <summary>
    ///     A provider's table reduced to the three things this class exercises. Explicit accessors
    ///     rather than a shared interface: <c>SetPrimaryKeyOrder</c> lives on the generic
    ///     <see cref="TableBase{TColumn,TIndex,TForeignKey}" />, and widening <see cref="ITable" />
    ///     to reach it from one non-generic place would be a breaking change for anyone outside this
    ///     repo who implements it. A test should not buy convenience with public API.
    /// </summary>
    private sealed record ProviderTable(
        string Provider,
        Func<IReadOnlyList<string>> Key,
        Action<IEnumerable<string>> Pin,
        Func<bool> HasPin);

    /// <summary>
    ///     A three-column composite key per provider, flagged in the order a, b, c.
    /// </summary>
    private static IEnumerable<ProviderTable> compositeKeyTables()
    {
        var pg = new Postgresql.Tables.Table("test.thing");
        foreach (var name in new[] { "a", "b", "c" }) pg.AddColumn<int>(name).AsPrimaryKey();
        yield return new ProviderTable("PostgreSQL", () => pg.PrimaryKeyColumns, pg.SetPrimaryKeyOrder,
            () => pg.HasExplicitPrimaryKeyOrder);

        var ss = new SqlServer.Tables.Table("test.thing");
        foreach (var name in new[] { "a", "b", "c" }) ss.AddColumn<int>(name).AsPrimaryKey();
        yield return new ProviderTable("SQL Server", () => ss.PrimaryKeyColumns, ss.SetPrimaryKeyOrder,
            () => ss.HasExplicitPrimaryKeyOrder);

        var lite = new Sqlite.Tables.Table("thing");
        foreach (var name in new[] { "a", "b", "c" }) lite.AddColumn<int>(name).AsPrimaryKey();
        yield return new ProviderTable("SQLite", () => lite.PrimaryKeyColumns, lite.SetPrimaryKeyOrder,
            () => lite.HasExplicitPrimaryKeyOrder);

        var my = new MySql.Tables.Table("test.thing");
        foreach (var name in new[] { "a", "b", "c" }) my.AddColumn<int>(name).AsPrimaryKey();
        yield return new ProviderTable("MySQL", () => my.PrimaryKeyColumns, my.SetPrimaryKeyOrder,
            () => my.HasExplicitPrimaryKeyOrder);

        var ora = new Oracle.Tables.Table("TEST.THING");
        foreach (var name in new[] { "a", "b", "c" }) ora.AddColumn<int>(name).AsPrimaryKey();
        yield return new ProviderTable("Oracle", () => ora.PrimaryKeyColumns, ora.SetPrimaryKeyOrder,
            () => ora.HasExplicitPrimaryKeyOrder);
    }

    private static ProviderTable tableFor(string provider)
        => compositeKeyTables().Single(x => x.Provider == provider);

    /// <summary>
    ///     The fixture itself: a provider whose key does not come back as (a, b, c) is not set up the
    ///     way the rest of this class assumes, and its other results would mean nothing.
    /// </summary>
    [Fact]
    public void the_unpinned_key_is_the_order_the_columns_were_flagged_in()
    {
        foreach (var t in compositeKeyTables())
        {
            t.Key().ShouldBe(["a", "b", "c"], $"{t.Provider} did not flag a, b, c in order");
        }
    }

    [Fact]
    public void a_pin_reorders_the_key_on_every_provider()
    {
        var offenders = new List<string>();

        foreach (var t in compositeKeyTables())
        {
            t.Pin(["c", "a", "b"]);

            if (!t.Key().SequenceEqual(["c", "a", "b"]))
            {
                offenders.Add($"{t.Provider}: got {string.Join(", ", t.Key())}, expected c, a, b");
            }
        }

        offenders.ShouldBeEmpty(
            $"PrimaryKeyColumns on these providers does not route through ApplyPrimaryKeyOrder, so a pinned key order is silently ignored:{Environment.NewLine}"
            + string.Join(Environment.NewLine, offenders));
    }

    [Fact]
    public void clearing_the_pin_restores_the_flagged_order_on_every_provider()
    {
        foreach (var t in compositeKeyTables())
        {
            t.Pin(["c", "a", "b"]);
            t.Pin([]);

            t.HasPin().ShouldBeFalse($"{t.Provider} kept the pin");
            t.Key().ShouldBe(["a", "b", "c"], $"{t.Provider} did not go back to flagged order");
        }
    }

    /// <summary>
    ///     The pin ORDERS the flagged set rather than replacing it, so a column dropped afterwards
    ///     cannot be resurrected by a pin that still names it.
    /// </summary>
    [Fact]
    public void a_pin_cannot_resurrect_a_column_that_is_no_longer_in_the_key()
    {
        var table = new Postgresql.Tables.Table("test.thing");
        foreach (var name in new[] { "a", "b", "c" }) table.AddColumn<int>(name).AsPrimaryKey();
        table.SetPrimaryKeyOrder(["c", "a", "b"]);

        table.RemoveColumn("a");

        table.PrimaryKeyColumns.ShouldBe(["c", "b"]);
    }

    public static IEnumerable<object[]> Providers()
    {
        yield return ["PostgreSQL"];
        yield return ["SQL Server"];
        yield return ["SQLite"];
        yield return ["MySQL"];
        yield return ["Oracle"];
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void a_pin_that_repeats_a_column_is_rejected(string provider)
    {
        var table = tableFor(provider);

        Should.Throw<ArgumentException>(() => table.Pin(["a", "a", "b"]))
            .Message.ShouldContain("repeats");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void a_pin_naming_a_column_outside_the_key_is_rejected(string provider)
    {
        var table = tableFor(provider);

        Should.Throw<ArgumentException>(() => table.Pin(["a", "b", "nope"]))
            .Message.ShouldContain("nope");
    }

    /// <summary>
    ///     A partial pin would still opt the table into strict order comparison while silently
    ///     reordering the columns it does not name.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void a_partial_pin_is_rejected(string provider)
    {
        var table = tableFor(provider);

        Should.Throw<ArgumentException>(() => table.Pin(["a", "b"]))
            .Message.ShouldContain("2 of 3");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void an_unpinned_table_reports_no_explicit_order(string provider)
    {
        var table = tableFor(provider);

        table.HasPin().ShouldBeFalse();

        table.Pin(["c", "a", "b"]);
        table.HasPin().ShouldBeTrue();
    }
}
