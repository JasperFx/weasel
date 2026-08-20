using JasperFx;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     weasel#477: SQLite's table rebuild was unreachable through the migrator, so a change that
///     needed one dropped the table and everything in it.
/// </summary>
/// <remarks>
///     <para>
///         <c>TableDelta</c> has always had the careful path — create a new table,
///         <c>INSERT INTO … SELECT</c> the surviving columns, drop the old one, rename, put the
///         indexes and triggers back. But it reports <see cref="SchemaPatchDifference.Invalid" />
///         for exactly the changes that need it, and <c>Migrator</c> answered <c>Invalid</c> by
///         dropping and recreating the table. Measured before the fix: one row before a column type
///         change, zero after, and a schema that looked entirely correct.
///     </para>
///     <para>
///         These go through the migrator deliberately. Driving <c>TableDelta.WriteUpdate</c>
///         directly always worked, which is exactly why the bug survived.
///     </para>
/// </remarks>
public class rebuild_preserves_data
{
    private readonly string _connectionString = $"Data Source={Path.GetTempFileName()};";

    private async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync();
        await conn.ResetSchemaAsync("main");
        return conn;
    }

    /// <summary>
    ///     Through the migrator, with <see cref="AutoCreate.All" /> — a rebuild still reports
    ///     <c>Invalid</c> and still needs <c>All</c>, because a change that removes a column really
    ///     does take that column's data. What weasel#477 changed is only what <c>All</c> then does.
    /// </summary>
    private static async Task applyAsync(SqliteConnection conn, Table table)
    {
        var migration = await SchemaMigration.DetermineAsync(conn, CancellationToken.None, table);
        await new SqliteMigrator().ApplyAllAsync(conn, migration, AutoCreate.All);
    }

    private static Table OrdersTable(string quantityType = "INTEGER")
    {
        var table = new Table("rb_orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("quantity", quantityType);
        table.AddColumn<string>("note");
        return table;
    }

    [Fact]
    public async Task a_column_type_change_keeps_the_rows()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, OrdersTable());

        await conn.CreateCommand("INSERT INTO rb_orders (id, quantity, note) VALUES (1, 5, 'first')")
            .ExecuteNonQueryAsync();
        await conn.CreateCommand("INSERT INTO rb_orders (id, quantity, note) VALUES (2, 9, 'second')")
            .ExecuteNonQueryAsync();

        await applyAsync(conn, OrdersTable("TEXT"));

        var count = await conn.CreateCommand("SELECT COUNT(*) FROM rb_orders").ExecuteScalarAsync();
        Convert.ToInt32(count).ShouldBe(2, "the rebuild dropped the table instead of copying it");

        var note = await conn.CreateCommand("SELECT note FROM rb_orders WHERE id = 2").ExecuteScalarAsync();
        note.ShouldBe("second");
    }

    [Fact]
    public async Task the_change_the_rebuild_was_for_actually_takes_effect()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, OrdersTable());
        await applyAsync(conn, OrdersTable("TEXT"));

        var expected = OrdersTable("TEXT");
        var delta = await expected.FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     A primary key change is the other route into the rebuild, and the one where copying is
    ///     least obviously safe — so it gets its own case rather than riding on the type change.
    /// </summary>
    [Fact]
    public async Task a_primary_key_change_keeps_the_rows()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, OrdersTable());

        await conn.CreateCommand("INSERT INTO rb_orders (id, quantity, note) VALUES (1, 5, 'kept')")
            .ExecuteNonQueryAsync();

        var repointed = new Table("rb_orders");
        repointed.AddColumn<int>("id");
        repointed.AddColumn("quantity", "INTEGER");
        repointed.AddColumn<string>("note").AsPrimaryKey();

        await applyAsync(conn, repointed);

        var note = await conn.CreateCommand("SELECT note FROM rb_orders").ExecuteScalarAsync();
        note.ShouldBe("kept");
    }

    /// <summary>
    ///     Indexes are recreated by the rebuild, and were before this — but the rebuild running at
    ///     all is new, so nothing had proven it end to end through the migrator.
    /// </summary>
    [Fact]
    public async Task indexes_come_back_after_the_rebuild()
    {
        await using var conn = await openAsync();

        var original = OrdersTable();
        original.Indexes.Add(new IndexDefinition("rb_orders_note_idx") { Columns = ["note"] });
        await applyAsync(conn, original);

        var retyped = OrdersTable("TEXT");
        retyped.Indexes.Add(new IndexDefinition("rb_orders_note_idx") { Columns = ["note"] });
        await applyAsync(conn, retyped);

        var count = await conn
            .CreateCommand("SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'rb_orders_note_idx'")
            .ExecuteScalarAsync();

        Convert.ToInt32(count).ShouldBe(1);
    }

    /// <summary>
    ///     A table that does not exist yet has nothing to preserve, so the ordinary create path is
    ///     the right one and <c>CanRebuildInPlace</c> says so.
    /// </summary>
    [Fact]
    public async Task a_table_that_does_not_exist_yet_is_not_a_rebuild()
    {
        await using var conn = await openAsync();

        var delta = await OrdersTable().FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
        delta.CanRebuildInPlace.ShouldBeFalse();
    }
}
