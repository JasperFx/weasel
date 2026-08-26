using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     <c>pragma_foreign_key_list</c> reports <c>to</c> as NULL for a constraint that omits the
///     referenced column list — <c>REFERENCES parent</c>, the ordinary way to write a foreign key.
///     Reading that NULL straight out of the reader threw, and it took the whole table read down
///     with it, not just the foreign key.
/// </summary>
public class foreign_key_introspection
{
    private static async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection($"Data Source={Path.GetTempFileName()};");
        await conn.OpenAsync();
        await conn.ResetSchemaAsync("main");
        return conn;
    }

    private static async Task executeAsync(SqliteConnection conn, string sql)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task an_implicit_referenced_column_resolves_to_the_parents_primary_key()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "create table fki_parent (id integer primary key, label text)");
        await executeAsync(conn,
            "create table fki_child (id integer primary key, parent_id integer references fki_parent)");

        var existing = await new Table("fki_child").FetchExistingAsync(conn);

        existing.ShouldNotBeNull();
        var fk = existing.ForeignKeys.ShouldHaveSingleItem();
        fk.LinkedTable.Name.ShouldBe("fki_parent");
        fk.ColumnNames.ShouldBe(["parent_id"]);
        fk.LinkedNames.ShouldBe(["id"]);
    }

    /// <summary>
    ///     The read threw before it reached the columns, so a table carrying such a foreign key was
    ///     unreadable in its entirety.
    /// </summary>
    [Fact]
    public async Task the_rest_of_the_table_is_still_read()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "create table fki_parent2 (id integer primary key)");
        await executeAsync(conn,
            "create table fki_child2 (id integer primary key, note text, parent_id integer references fki_parent2)");

        var existing = await new Table("fki_child2").FetchExistingAsync(conn);

        existing.ShouldNotBeNull();
        existing.Columns.Select(x => x.Name).ShouldBe(["id", "note", "parent_id"]);
    }

    [Fact]
    public async Task an_implicit_reference_to_a_composite_key_pairs_by_key_position()
    {
        await using var conn = await openAsync();
        await executeAsync(conn,
            "create table fki_ckey (a int not null, b int not null, primary key (b, a))");
        await executeAsync(conn,
            "create table fki_cchild (id integer primary key, x int, y int, foreign key (x, y) references fki_ckey)");

        var existing = await new Table("fki_cchild").FetchExistingAsync(conn);

        existing.ShouldNotBeNull();
        var fk = existing.ForeignKeys.ShouldHaveSingleItem();
        // The key is declared (b, a), so x pairs with b and y with a. Zipped before the order is
        // ignored: ignoreOrder on LinkedNames alone passes for the (x->a, y->b) mispairing too,
        // which is the one thing this test exists to catch.
        fk.ColumnNames.Zip(fk.LinkedNames, (column, linked) => $"{column}->{linked}")
            .ShouldBe(["x->b", "y->a"], ignoreOrder: true);
    }

    [Fact]
    public async Task a_reference_to_a_table_with_no_primary_key_is_left_out()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "create table fki_keyless (v text)");
        await executeAsync(conn,
            "create table fki_dangling (id integer primary key, v text references fki_keyless)");

        var existing = await new Table("fki_dangling").FetchExistingAsync(conn);

        existing.ShouldNotBeNull();
        existing.Columns.Select(x => x.Name).ShouldBe(["id", "v"]);
        existing.ForeignKeys.ShouldBeEmpty();
    }

    [Fact]
    public async Task an_explicit_referenced_column_is_still_read()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "create table fki_parent3 (id integer primary key, code text unique)");
        await executeAsync(conn,
            "create table fki_child3 (id integer primary key, parent_code text references fki_parent3(code))");

        var existing = await new Table("fki_child3").FetchExistingAsync(conn);

        existing.ShouldNotBeNull();
        var fk = existing.ForeignKeys.ShouldHaveSingleItem();
        fk.ColumnNames.ShouldBe(["parent_code"]);
        fk.LinkedNames.ShouldBe(["code"]);
    }

    [Fact]
    public async Task referential_actions_are_still_read()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "create table fki_parent4 (id integer primary key)");
        await executeAsync(conn,
            "create table fki_child4 (id integer primary key, parent_id integer references fki_parent4 on delete cascade on update set null)");

        var existing = await new Table("fki_child4").FetchExistingAsync(conn);

        var fk = existing!.ForeignKeys.ShouldHaveSingleItem();
        fk.DeleteAction.ShouldBe(CascadeAction.Cascade);
        fk.UpdateAction.ShouldBe(CascadeAction.SetNull);
    }

    [Fact]
    public async Task several_foreign_keys_are_all_read()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "create table fki_a (id integer primary key)");
        await executeAsync(conn, "create table fki_b (id integer primary key)");
        await executeAsync(conn,
            "create table fki_multi (id integer primary key, a_id integer references fki_a, b_id integer references fki_b(id))");

        var existing = await new Table("fki_multi").FetchExistingAsync(conn);

        existing!.ForeignKeys.Count.ShouldBe(2);
        existing.ForeignKeys.Select(x => x.LinkedTable.Name).ShouldBe(["fki_a", "fki_b"], ignoreOrder: true);
    }
}
