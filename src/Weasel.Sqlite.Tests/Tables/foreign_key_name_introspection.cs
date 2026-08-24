using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     <c>pragma_foreign_key_list</c> has no name column, so the introspection made one up:
///     <c>fk_{table}_{reftable}_{id}</c>. Every foreign key Weasel itself had written therefore read
///     back under a name the model did not have — the delta saw one constraint missing and one
///     extra, and on SQLite that is repaired by rebuilding the table and copying every row. The read
///     could never converge, so it happened on every run.
/// </summary>
public class foreign_key_name_introspection
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

    private static async Task<Table> parentAsync(SqliteConnection conn, string name)
    {
        var parent = new Table(name);
        parent.AddColumn<int>("id").AsPrimaryKey();
        await parent.CreateAsync(conn);
        return parent;
    }

    private static Table childWith(string name, string fkName, string parentName)
    {
        var child = new Table(name);
        child.AddColumn<int>("id").AsPrimaryKey();
        child.AddColumn<int>("parent_id");

        var fk = new ForeignKey(fkName) { LinkedTable = new SqliteObjectName(parentName) };
        fk.LinkColumns("parent_id", "id");
        child.ForeignKeys.Add(fk);

        return child;
    }

    [Fact]
    public async Task the_declared_constraint_name_is_read_back()
    {
        await using var conn = await openAsync();
        await parentAsync(conn, "fkn_customers");

        var child = childWith("fkn_orders", "fk_orders_to_customers", "fkn_customers");
        await child.CreateAsync(conn);

        var existing = await child.FetchExistingAsync(conn);

        existing!.ForeignKeys.ShouldHaveSingleItem().Name.ShouldBe("fk_orders_to_customers");
    }

    [Fact]
    public async Task a_table_with_a_named_foreign_key_converges()
    {
        await using var conn = await openAsync();
        await parentAsync(conn, "fkn_customers2");

        var child = childWith("fkn_orders2", "fk_orders_to_customers", "fkn_customers2");
        await child.CreateAsync(conn);

        var delta = await child.FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
        delta.ForeignKeys.Missing.ShouldBeEmpty();
        delta.ForeignKeys.Extras.ShouldBeEmpty();
    }

    [Fact]
    public async Task several_named_foreign_keys_keep_their_own_names()
    {
        await using var conn = await openAsync();
        await parentAsync(conn, "fkn_a");
        await parentAsync(conn, "fkn_b");

        var child = new Table("fkn_two");
        child.AddColumn<int>("id").AsPrimaryKey();
        child.AddColumn<int>("a_id");
        child.AddColumn<int>("b_id");

        var toA = new ForeignKey("fk_two_a") { LinkedTable = new SqliteObjectName("fkn_a") };
        toA.LinkColumns("a_id", "id");
        var toB = new ForeignKey("fk_two_b") { LinkedTable = new SqliteObjectName("fkn_b") };
        toB.LinkColumns("b_id", "id");
        child.ForeignKeys.Add(toA);
        child.ForeignKeys.Add(toB);

        await child.CreateAsync(conn);

        var existing = await child.FetchExistingAsync(conn);

        existing!.ForeignKeys.Select(x => x.Name).ShouldBe(["fk_two_a", "fk_two_b"], ignoreOrder: true);
        (await child.FindDeltaAsync(conn)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_renamed_constraint_is_still_drift()
    {
        await using var conn = await openAsync();
        await parentAsync(conn, "fkn_customers3");

        await childWith("fkn_orders3", "fk_the_old_name", "fkn_customers3").CreateAsync(conn);

        var delta = await childWith("fkn_orders3", "fk_the_new_name", "fkn_customers3").FindDeltaAsync(conn);

        delta.Difference.ShouldNotBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     An inline <c>REFERENCES</c> has no name in the DDL to read, so the synthesised one stands.
    /// </summary>
    [Fact]
    public async Task an_unnamed_constraint_keeps_the_synthesised_name()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "create table fkn_parent4 (id integer primary key)");
        await executeAsync(conn,
            "create table fkn_child4 (id integer primary key, parent_id integer references fkn_parent4(id))");

        var existing = await new Table("fkn_child4").FetchExistingAsync(conn);

        existing!.ForeignKeys.ShouldHaveSingleItem().Name.ShouldBe("fk_fkn_child4_fkn_parent4_0");
    }
}
