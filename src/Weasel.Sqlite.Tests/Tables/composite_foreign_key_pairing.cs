using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     A foreign key pairs its columns positionally. Sorting the dependent and the referenced list
///     independently — which the property setters did — kept both lists tidy and destroyed the
///     pairing between them: <c>(x, y) REFERENCES parent (b, a)</c> was emitted as
///     <c>(x, y) REFERENCES parent (a, b)</c>, a constraint over different columns entirely.
/// </summary>
public class composite_foreign_key_pairing
{
    private static async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection($"Data Source={Path.GetTempFileName()};");
        await conn.OpenAsync();
        await conn.ResetSchemaAsync("main");
        return conn;
    }

    private static ForeignKey pairedKey(string name = "fk_pair")
    {
        var fk = new ForeignKey(name) { LinkedTable = new SqliteObjectName("cfk_parent") };
        fk.LinkColumns("x", "b");
        fk.LinkColumns("y", "a");
        return fk;
    }

    [Fact]
    public void the_pairing_survives_being_set()
    {
        var fk = pairedKey();

        fk.ColumnNames.ShouldBe(["x", "y"]);
        fk.LinkedNames.ShouldBe(["b", "a"]);
    }

    [Fact]
    public void the_declaration_pairs_the_columns_as_declared()
    {
        var writer = new StringWriter();
        pairedKey().WriteInlineDefinition(writer);

        writer.ToString().ShouldBe("CONSTRAINT fk_pair FOREIGN KEY (x, y) REFERENCES cfk_parent (b, a)");
    }

    [Fact]
    public async Task a_composite_key_round_trips_with_its_pairing_intact()
    {
        await using var conn = await openAsync();

        var parent = new Table("cfk_parent");
        parent.AddColumn<int>("a").AsPrimaryKey();
        parent.AddColumn<int>("b").AsPrimaryKey();
        await parent.CreateAsync(conn);

        var child = new Table("cfk_child");
        child.AddColumn<int>("id").AsPrimaryKey();
        child.AddColumn<int>("x");
        child.AddColumn<int>("y");
        child.ForeignKeys.Add(pairedKey());
        await child.CreateAsync(conn);

        var existing = await child.FetchExistingAsync(conn);

        var fk = existing!.ForeignKeys.ShouldHaveSingleItem();
        fk.ColumnNames.Zip(fk.LinkedNames, (column, linked) => $"{column}->{linked}")
            .ShouldBe(["x->b", "y->a"], ignoreOrder: true);
    }

    [Fact]
    public async Task a_composite_key_converges_against_its_own_table()
    {
        await using var conn = await openAsync();

        var parent = new Table("cfk_parent");
        parent.AddColumn<int>("a").AsPrimaryKey();
        parent.AddColumn<int>("b").AsPrimaryKey();
        await parent.CreateAsync(conn);

        var child = new Table("cfk_child2");
        child.AddColumn<int>("id").AsPrimaryKey();
        child.AddColumn<int>("x");
        child.AddColumn<int>("y");
        child.ForeignKeys.Add(pairedKey());
        await child.CreateAsync(conn);

        var delta = await child.FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    /// <summary>
    ///     Upgrade safety: the same key written in the other order is the same constraint, and must
    ///     not read as drift now that the lists are no longer normalised by sorting.
    /// </summary>
    [Fact]
    public void the_same_pairs_written_in_another_order_are_equal()
    {
        var one = new ForeignKey("fk_same") { LinkedTable = new SqliteObjectName("cfk_parent") };
        one.LinkColumns("x", "b");
        one.LinkColumns("y", "a");

        var other = new ForeignKey("fk_same") { LinkedTable = new SqliteObjectName("cfk_parent") };
        other.LinkColumns("y", "a");
        other.LinkColumns("x", "b");

        one.ShouldBe(other);
    }

    [Fact]
    public void differently_paired_columns_are_not_equal()
    {
        var one = new ForeignKey("fk_diff") { LinkedTable = new SqliteObjectName("cfk_parent") };
        one.LinkColumns("x", "b");
        one.LinkColumns("y", "a");

        var other = new ForeignKey("fk_diff") { LinkedTable = new SqliteObjectName("cfk_parent") };
        other.LinkColumns("x", "a");
        other.LinkColumns("y", "b");

        one.ShouldNotBe(other);
    }

    [Fact]
    public void a_single_column_key_is_unaffected()
    {
        var one = new ForeignKey("fk_one") { LinkedTable = new SqliteObjectName("cfk_parent") };
        one.LinkColumns("x", "a");

        var other = new ForeignKey("fk_one") { LinkedTable = new SqliteObjectName("cfk_parent") };
        other.LinkColumns("x", "a");

        one.ShouldBe(other);
        one.ColumnNames.ShouldBe(["x"]);
        one.LinkedNames.ShouldBe(["a"]);
    }
}
