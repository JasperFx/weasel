using JasperFx;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     The rollback of a table rebuild dropped the table and recreated its previous shape empty.
/// </summary>
/// <remarks>
///     <para>
///         weasel#477 made the forward migration rebuild the table in place instead of dropping it,
///         but <see cref="SchemaMigration.WriteAllRollbacks" /> still answered the same
///         <c>Invalid</c> delta with a <c>DROP TABLE</c> and the old <c>CREATE TABLE</c>. So the drop
///         file <c>db-patch</c> writes beside every rebuild, and <c>RollbackAllAsync</c>, emptied the
///         table. <c>TableDelta</c> had a reverse rebuild all along; nothing reached it.
///     </para>
///     <para>
///         These go through the two public rollback paths rather than calling
///         <c>TableDelta.WriteRollback</c> directly, which is exactly why the defect survived.
///     </para>
/// </remarks>
public class rebuild_rollback_keeps_the_rows
{
    private readonly string _connectionString = $"Data Source={Path.GetTempFileName()};";

    private static CancellationToken ct => TestContext.Current.CancellationToken;

    private async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync(ct);
        await conn.ResetSchemaAsync("main", ct);
        await executeAsync(conn, "PRAGMA foreign_keys = ON");
        return conn;
    }

    private static async Task executeAsync(SqliteConnection conn, string sql)
    {
        await conn.CreateCommand(sql).ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> scalarAsync(SqliteConnection conn, string sql)
    {
        return await conn.CreateCommand(sql).ExecuteScalarAsync(ct);
    }

    private static async Task<int> countAsync(SqliteConnection conn, string sql)
    {
        return Convert.ToInt32(await scalarAsync(conn, sql));
    }

    private static async Task<string?> quantityTypeAsync(SqliteConnection conn)
    {
        return await scalarAsync(conn, "SELECT type FROM pragma_table_info('rr_orders') WHERE name = 'quantity'") as string;
    }

    /// <summary>
    ///     <paramref name="quantityType" /> is the lever: SQLite can only change a column's type by
    ///     rebuilding the table.
    /// </summary>
    private static Table Orders(string quantityType = "INTEGER", string noteName = "note")
    {
        var table = new Table("rr_orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("quantity", quantityType);
        table.AddColumn<string>(noteName);
        return table;
    }

    /// <summary>
    ///     The table as it was, holding two rows, rebuilt by the migration returned with it.
    ///     <paramref name="around" /> runs before the migration is determined.
    /// </summary>
    private async Task<(SqliteConnection Connection, SchemaMigration Migration)> rebuiltAsync(Table target,
        params string[] around)
    {
        var conn = await openAsync();

        var create = await SchemaMigration.DetermineAsync(conn, ct, Orders());
        await new SqliteMigrator().ApplyAllAsync(conn, create, AutoCreate.CreateOrUpdate, ct: ct);
        await executeAsync(conn, "INSERT INTO rr_orders (id, quantity, note) VALUES (1, 5, 'first'), (2, 9, 'second')");

        foreach (var sql in around)
        {
            await executeAsync(conn, sql);
        }

        var migration = await SchemaMigration.DetermineAsync(conn, ct, target);
        migration.Deltas.Single().ShouldBeOfType<TableDelta>().CanRebuildInPlace.ShouldBeTrue();

        await new SqliteMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct);
        (await quantityTypeAsync(conn)).ShouldBe(target.ColumnFor("quantity")!.Type, "the rebuild has to have run");

        return (conn, migration);
    }

    [Fact]
    public async Task rolling_back_a_rebuild_keeps_the_rows()
    {
        var (conn, migration) = await rebuiltAsync(Orders("TEXT"));
        await using var _ = conn;

        await migration.RollbackAllAsync(conn, new SqliteMigrator(), ct);

        (await quantityTypeAsync(conn)).ShouldBe("INTEGER");
        (await countAsync(conn, "SELECT COUNT(*) FROM rr_orders")).ShouldBe(2, "the rollback emptied the table");
        (await scalarAsync(conn, "SELECT note FROM rr_orders WHERE id = 2")).ShouldBe("second");
    }

    /// <summary>
    ///     What <c>db-patch</c> writes: the migration file and its companion drop file. Run the drop
    ///     file as a script after the migration and the rows have to still be there.
    /// </summary>
    [Fact]
    public async Task the_drop_file_db_patch_writes_keeps_the_rows()
    {
        var file = Path.Combine(Path.GetTempPath(), $"rr_{Guid.NewGuid():N}.sql");
        var (conn, migration) = await rebuiltAsync(Orders("TEXT"));
        await using var _ = conn;

        await new SqliteMigrator().WriteMigrationFileAsync(file, migration, ct);
        var dropFile = SchemaMigration.ToDropFileName(file);

        try
        {
            await executeAsync(conn, await File.ReadAllTextAsync(dropFile, ct));
        }
        finally
        {
            File.Delete(file);
            File.Delete(dropFile);
        }

        (await quantityTypeAsync(conn)).ShouldBe("INTEGER");
        (await countAsync(conn, "SELECT COUNT(*) FROM rr_orders")).ShouldBe(2, "the drop file emptied the table");
    }

    /// <summary>
    ///     A reverse rebuild is a rebuild, so <c>RollbackAllAsync</c> has to run it the way
    ///     <c>ApplyAllAsync</c> runs the forward one. Run bare, <c>DROP TABLE</c> cascaded into the
    ///     table referencing it, and the rename back failed on the view — after the drop, leaving no
    ///     table at all.
    /// </summary>
    [Fact]
    public async Task the_rollback_is_run_as_a_rebuild_around_views_and_referencing_tables()
    {
        var (conn, migration) = await rebuiltAsync(Orders("TEXT"),
            "CREATE VIEW rr_order_ids AS SELECT id FROM rr_orders",
            "CREATE TABLE rr_lines (id INTEGER PRIMARY KEY, order_id INTEGER REFERENCES rr_orders (id) ON DELETE CASCADE)",
            "INSERT INTO rr_lines (id, order_id) VALUES (1, 1)");
        await using var _ = conn;

        await migration.RollbackAllAsync(conn, new SqliteMigrator(), ct);

        (await quantityTypeAsync(conn)).ShouldBe("INTEGER");
        (await countAsync(conn, "SELECT COUNT(*) FROM rr_order_ids")).ShouldBe(2);
        (await countAsync(conn, "SELECT COUNT(*) FROM rr_lines")).ShouldBe(1, "DROP TABLE cascaded into rr_lines");
        (await countAsync(conn, "SELECT COUNT(*) FROM pragma_foreign_key_check")).ShouldBe(0);
        (await countAsync(conn, "PRAGMA foreign_keys")).ShouldBe(1, "foreign key enforcement was not restored");
    }

    /// <summary>
    ///     A rename inside a rebuild is copied forward from the old name, so it has to be copied back
    ///     to it.
    /// </summary>
    [Fact]
    public async Task a_column_renamed_by_the_rebuild_is_copied_back_under_its_old_name()
    {
        var (conn, migration) = await rebuiltAsync(Orders("TEXT", noteName: "remark"));
        await using var _ = conn;
        (await scalarAsync(conn, "SELECT remark FROM rr_orders WHERE id = 1")).ShouldBe("first");

        await migration.RollbackAllAsync(conn, new SqliteMigrator(), ct);

        (await scalarAsync(conn, "SELECT note FROM rr_orders WHERE id = 1")).ShouldBe("first");
    }

    /// <summary>
    ///     <c>DROP TABLE</c> takes the table's indexes and triggers with it, going back as much as
    ///     going forward (weasel#452).
    /// </summary>
    [Fact]
    public async Task indexes_and_triggers_come_back_with_the_rollback()
    {
        var conn = await openAsync();
        await using var _ = conn;

        var original = Orders();
        original.Indexes.Add(new IndexDefinition("rr_orders_note_idx") { Columns = ["note"] });
        await new SqliteMigrator().ApplyAllAsync(conn,
            await SchemaMigration.DetermineAsync(conn, ct, original), AutoCreate.CreateOrUpdate, ct: ct);
        await executeAsync(conn, "CREATE TABLE rr_audit (order_id INTEGER)");
        await executeAsync(conn,
            "CREATE TRIGGER rr_orders_audit AFTER UPDATE ON rr_orders BEGIN INSERT INTO rr_audit VALUES (NEW.id); END");
        await executeAsync(conn, "INSERT INTO rr_orders (id, quantity, note) VALUES (1, 5, 'first')");

        var retyped = Orders("TEXT");
        retyped.Indexes.Add(new IndexDefinition("rr_orders_note_idx") { Columns = ["note"] });
        var migration = await SchemaMigration.DetermineAsync(conn, ct, retyped);
        await new SqliteMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct);

        await migration.RollbackAllAsync(conn, new SqliteMigrator(), ct);

        (await countAsync(conn, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'rr_orders_note_idx'"))
            .ShouldBe(1);
        await executeAsync(conn, "UPDATE rr_orders SET note = 'touched'");
        (await countAsync(conn, "SELECT COUNT(*) FROM rr_audit")).ShouldBe(1, "the trigger was not put back");
    }

    /// <summary>
    ///     SQLite refuses a write to a generated column, so the reverse copy has to leave one out
    ///     exactly as the forward copy does: by what the recreated table declares.
    /// </summary>
    /// <remarks>
    ///     The catalog read does not recover a generation expression today, so a table read back from
    ///     the database recreates such a column as a plain one and copies its values in. The previous
    ///     shape is built by hand here to be the one the rollback sees once it does -- the case in
    ///     which the copy would otherwise fail.
    /// </remarks>
    [Fact]
    public async Task a_generated_column_in_the_previous_shape_is_left_out_of_the_copy_back()
    {
        var conn = await openAsync();
        await using var _ = conn;

        Table Scores(string pointsType)
        {
            var table = new Table("rr_scores");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn("points", pointsType);
            table.AddColumn("id_twice", "INTEGER").GeneratedAs("id * 2");
            return table;
        }

        var current = Scores("TEXT");
        await new SqliteMigrator().ApplyAllAsync(conn,
            await SchemaMigration.DetermineAsync(conn, ct, current), AutoCreate.CreateOrUpdate, ct: ct);
        await executeAsync(conn, "INSERT INTO rr_scores (id, points) VALUES (1, '5'), (2, '9')");

        var rollback = new SchemaMigration(new TableDelta(current, Scores("INTEGER")));
        await rollback.RollbackAllAsync(conn, new SqliteMigrator(), ct);

        (await scalarAsync(conn, "SELECT type FROM pragma_table_info('rr_scores') WHERE name = 'points'")).ShouldBe("INTEGER");
        (await countAsync(conn, "SELECT id_twice FROM rr_scores WHERE id = 2")).ShouldBe(4);
        (await countAsync(conn, "SELECT hidden FROM pragma_table_xinfo('rr_scores') WHERE name = 'id_twice'"))
            .ShouldBe(2, "still a VIRTUAL generated column");
    }

    /// <summary>
    ///     The reverse rebuild creates a new table too, so it has to carry the <c>AUTOINCREMENT</c>
    ///     high-water mark across just as the forward one does, or the rolled-back table reissues ids
    ///     it already handed out.
    /// </summary>
    [Fact]
    public async Task an_autoincrement_table_does_not_reissue_ids_after_the_rollback()
    {
        var conn = await openAsync();
        await using var _ = conn;

        Table Tickets(string labelType)
        {
            var table = new Table("rr_tickets");
            table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
            table.AddColumn("label", labelType);
            return table;
        }

        await new SqliteMigrator().ApplyAllAsync(conn,
            await SchemaMigration.DetermineAsync(conn, ct, Tickets("TEXT")), AutoCreate.CreateOrUpdate, ct: ct);
        await executeAsync(conn, "INSERT INTO rr_tickets (label) VALUES ('a'), ('b'), ('c')");
        await executeAsync(conn, "DELETE FROM rr_tickets WHERE id = 3");

        var migration = await SchemaMigration.DetermineAsync(conn, ct, Tickets("BLOB"));
        await new SqliteMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate, ct: ct);
        await migration.RollbackAllAsync(conn, new SqliteMigrator(), ct);

        await executeAsync(conn, "INSERT INTO rr_tickets (label) VALUES ('d')");
        Convert.ToInt64(await scalarAsync(conn, "SELECT id FROM rr_tickets WHERE label = 'd'")).ShouldBe(4);
    }
}
