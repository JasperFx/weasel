using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     <c>pragma_table_xinfo.pk</c> is the column's 1-based position within the primary key, not a
///     flag. Reading it as a flag returned the key in table column order, so a key declared
///     <c>PRIMARY KEY (b, a)</c> read back as <c>(a, b)</c> — and on SQLite a key that compares
///     unequal is repaired by rebuilding the table and copying every row into it.
/// </summary>
public class composite_primary_key_order
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
    public async Task declared_key_order_is_read_back_faithfully()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "create table pk_order (a int not null, b int not null, primary key (b, a))");

        var existing = await new Table("pk_order").FetchExistingAsync(conn);

        existing.ShouldNotBeNull();
        existing.PrimaryKeyColumns.ShouldBe(["b", "a"]);
    }

    [Fact]
    public async Task three_column_key_is_read_back_faithfully()
    {
        await using var conn = await openAsync();
        await executeAsync(conn,
            "create table pk_three (a int not null, b int not null, c int not null, primary key (c, a, b))");

        var existing = await new Table("pk_three").FetchExistingAsync(conn);

        existing.ShouldNotBeNull();
        existing.PrimaryKeyColumns.ShouldBe(["c", "a", "b"]);
    }

    [Fact]
    public async Task a_key_flagged_out_of_column_order_converges_against_its_own_table()
    {
        await using var conn = await openAsync();

        var table = new Table("pk_reversed");
        var a = table.AddColumn<int>("a");
        var b = table.AddColumn<int>("b");
        b.AsPrimaryKey();
        a.AsPrimaryKey();

        table.PrimaryKeyColumns.ShouldBe(["b", "a"]);
        await table.CreateAsync(conn);

        var delta = await table.FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    [Fact]
    public async Task a_pinned_order_is_read_back_and_converges()
    {
        await using var conn = await openAsync();

        var table = new Table("pk_pinned");
        table.AddColumn<int>("a").AsPrimaryKey();
        table.AddColumn<int>("b").AsPrimaryKey();
        table.SetPrimaryKeyOrder(["b", "a"]);

        table.PrimaryKeyColumns.ShouldBe(["b", "a"]);
        await table.CreateAsync(conn);

        var existing = await table.FetchExistingAsync(conn);
        existing!.PrimaryKeyColumns.ShouldBe(["b", "a"]);

        var delta = await table.FindDeltaAsync(conn);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    [Fact]
    public async Task a_pin_that_disagrees_with_the_database_is_drift()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "create table pk_pin_drift (a int not null, b int not null, primary key (a, b))");

        var table = new Table("pk_pin_drift");
        table.AddColumn<int>("a").AsPrimaryKey();
        table.AddColumn<int>("b").AsPrimaryKey();
        table.SetPrimaryKeyOrder(["b", "a"]);

        var delta = await table.FindDeltaAsync(conn);

        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);
    }

    /// <summary>
    ///     Upgrade safety. A key the database declares in an order the model cannot express — the
    ///     model only ever flags columns — must not start reporting drift, because the repair on
    ///     SQLite is a full table rebuild.
    /// </summary>
    [Fact]
    public async Task an_unpinned_model_does_not_drift_against_a_reordered_key()
    {
        await using var conn = await openAsync();
        await executeAsync(conn, "create table pk_legacy (a int not null, b int not null, primary key (b, a))");

        var table = new Table("pk_legacy");
        table.AddColumn<int>("a").AsPrimaryKey();
        table.AddColumn<int>("b").AsPrimaryKey();

        var delta = await table.FindDeltaAsync(conn);

        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.None);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    [Fact]
    public async Task a_key_in_declaration_order_still_converges()
    {
        await using var conn = await openAsync();

        var table = new Table("pk_plain");
        table.AddColumn<int>("a").AsPrimaryKey();
        table.AddColumn<int>("b").AsPrimaryKey();
        await table.CreateAsync(conn);

        var delta = await table.FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    [Fact]
    public async Task a_genuinely_different_key_is_still_drift()
    {
        await using var conn = await openAsync();
        await executeAsync(conn,
            "create table pk_changed (a int not null, b int not null, c int not null, primary key (a, b))");

        var table = new Table("pk_changed");
        table.AddColumn<int>("a").AsPrimaryKey();
        table.AddColumn<int>("b");
        table.AddColumn<int>("c").AsPrimaryKey();

        var delta = await table.FindDeltaAsync(conn);

        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public void a_pin_naming_a_column_outside_the_key_is_rejected()
    {
        var table = new Table("pk_bad");
        table.AddColumn<int>("a").AsPrimaryKey();
        table.AddColumn<int>("b").AsPrimaryKey();
        table.AddColumn<int>("c");

        Should.Throw<ArgumentException>(() => table.SetPrimaryKeyOrder(["a", "c"]));
    }

    [Fact]
    public void a_partial_pin_is_rejected()
    {
        var table = new Table("pk_partial");
        table.AddColumn<int>("a").AsPrimaryKey();
        table.AddColumn<int>("b").AsPrimaryKey();

        Should.Throw<ArgumentException>(() => table.SetPrimaryKeyOrder(["b"]));
    }

    [Fact]
    public void a_pin_that_repeats_a_column_is_rejected()
    {
        var table = new Table("pk_dupe");
        table.AddColumn<int>("a").AsPrimaryKey();
        table.AddColumn<int>("b").AsPrimaryKey();

        Should.Throw<ArgumentException>(() => table.SetPrimaryKeyOrder(["a", "a"]));
    }

    [Fact]
    public void an_empty_pin_clears_the_pin()
    {
        var table = new Table("pk_clear");
        table.AddColumn<int>("a").AsPrimaryKey();
        table.AddColumn<int>("b").AsPrimaryKey();
        table.SetPrimaryKeyOrder(["b", "a"]);
        table.HasExplicitPrimaryKeyOrder.ShouldBeTrue();

        table.SetPrimaryKeyOrder([]);

        table.HasExplicitPrimaryKeyOrder.ShouldBeFalse();
        table.PrimaryKeyColumns.ShouldBe(["a", "b"]);
    }
}
