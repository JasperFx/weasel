using JasperFx;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     SQLite's table rebuild ran as four bare autocommitted statements with no foreign key handling,
///     so rebuilding a table another table references wedged the database — the <c>DROP TABLE</c>
///     failed on its implicit delete, the <c>_new</c> table survived, and because it is created
///     <c>IF NOT EXISTS</c> every later start failed differently rather than healing. The same rebuild
///     also lost an <c>AUTOINCREMENT</c> table's <c>sqlite_sequence</c> high-water mark, silently
///     breaking the no-reuse promise that keyword exists for.
/// </summary>
public class rebuild_is_atomic
{
    private readonly string _connectionString = $"Data Source={Path.GetTempFileName()};";

    private async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync();
        await conn.ResetSchemaAsync("main");
        return conn;
    }

    private static async Task applyAsync(SqliteConnection conn, params Table[] tables)
    {
        var migration = await SchemaMigration.DetermineAsync(conn, CancellationToken.None, tables);
        await new SqliteMigrator().ApplyAllAsync(conn, migration, AutoCreate.All);
    }

    private static async Task executeAsync(SqliteConnection conn, string sql)
    {
        await conn.CreateCommand(sql).ExecuteNonQueryAsync();
    }

    private static async Task<object?> scalarAsync(SqliteConnection conn, string sql)
    {
        return await conn.CreateCommand(sql).ExecuteScalarAsync();
    }

    private static async Task<int> countAsync(SqliteConnection conn, string sql)
    {
        return Convert.ToInt32(await scalarAsync(conn, sql));
    }

    private static async Task<bool> tableExistsAsync(SqliteConnection conn, string name)
    {
        return await countAsync(conn, $"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '{name}'") > 0;
    }

    private static async Task<int> danglingReferencesAsync(SqliteConnection conn)
    {
        return await countAsync(conn, "SELECT COUNT(*) FROM pragma_foreign_key_check");
    }

    private static Table ParentTable(string noteType = "TEXT")
    {
        var table = new Table("ra_parent");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("note", noteType);
        return table;
    }

    private static Table ChildTable()
    {
        var table = new Table("ra_child");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("parent_id").ForeignKeyTo("ra_parent", "id");
        return table;
    }

    /// <summary>
    ///     <c>Microsoft.Data.Sqlite</c> enforces foreign keys on an ordinary connection, which is what
    ///     made the wedge the common case rather than an exotic one.
    /// </summary>
    [Fact]
    public async Task foreign_keys_are_enforced_on_a_plain_connection()
    {
        await using var conn = await openAsync();

        Convert.ToInt64(await scalarAsync(conn, "PRAGMA foreign_keys")).ShouldBe(1L);
    }

    [Fact]
    public async Task rebuilding_a_referenced_table_keeps_the_rows_and_the_reference()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, ParentTable(), ChildTable());

        await executeAsync(conn, "INSERT INTO ra_parent (id, note) VALUES (1, 'kept')");
        await executeAsync(conn, "INSERT INTO ra_child (id, parent_id) VALUES (1, 1)");

        await applyAsync(conn, ParentTable("INTEGER"), ChildTable());

        (await countAsync(conn, "SELECT COUNT(*) FROM ra_parent")).ShouldBe(1);
        (await scalarAsync(conn, "SELECT note FROM ra_parent WHERE id = 1")).ShouldBe("kept");
        (await countAsync(conn,
            "SELECT COUNT(*) FROM ra_child c INNER JOIN ra_parent p ON p.id = c.parent_id")).ShouldBe(1);
        (await danglingReferencesAsync(conn)).ShouldBe(0);
    }

    [Fact]
    public async Task rebuilding_a_referenced_table_leaves_no_orphan_table_behind()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, ParentTable(), ChildTable());

        await executeAsync(conn, "INSERT INTO ra_parent (id, note) VALUES (1, 'kept')");
        await executeAsync(conn, "INSERT INTO ra_child (id, parent_id) VALUES (1, 1)");

        await applyAsync(conn, ParentTable("INTEGER"), ChildTable());

        (await tableExistsAsync(conn, "ra_parent_new")).ShouldBeFalse();
    }

    [Fact]
    public async Task rebuilding_a_referenced_table_converges_on_the_second_apply()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, ParentTable(), ChildTable());

        await executeAsync(conn, "INSERT INTO ra_parent (id, note) VALUES (1, 'kept')");
        await executeAsync(conn, "INSERT INTO ra_child (id, parent_id) VALUES (1, 1)");

        await applyAsync(conn, ParentTable("INTEGER"), ChildTable());

        var migration = await SchemaMigration.DetermineAsync(
            conn, CancellationToken.None, ParentTable("INTEGER"), ChildTable());

        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Adding a foreign key to rows that already violate it is a rebuild that genuinely cannot
    ///     succeed. With enforcement suspended for the rebuild nothing else would notice, so it has to
    ///     fail on the explicit check and take the whole transaction with it.
    /// </summary>
    [Fact]
    public async Task a_rebuild_that_would_dangle_a_reference_is_refused()
    {
        await using var conn = await openAsync();

        var unconstrained = new Table("ra_child");
        unconstrained.AddColumn<int>("id").AsPrimaryKey();
        unconstrained.AddColumn<int>("parent_id");

        await applyAsync(conn, ParentTable(), unconstrained);
        await executeAsync(conn, "INSERT INTO ra_child (id, parent_id) VALUES (1, 99)");

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            () => applyAsync(conn, ParentTable(), ChildTable()));

        ex.Message.ShouldContain("ra_child");
        ex.Message.ShouldContain("rolled back");
    }

    [Fact]
    public async Task a_refused_rebuild_writes_nothing_at_all()
    {
        await using var conn = await openAsync();

        var unconstrained = new Table("ra_child");
        unconstrained.AddColumn<int>("id").AsPrimaryKey();
        unconstrained.AddColumn<int>("parent_id");

        await applyAsync(conn, ParentTable(), unconstrained);
        await executeAsync(conn, "INSERT INTO ra_child (id, parent_id) VALUES (1, 99)");

        try
        {
            await applyAsync(conn, ParentTable(), ChildTable());
        }
        catch (Exception)
        {
            // the assertions below are the point
        }

        (await tableExistsAsync(conn, "ra_child_new")).ShouldBeFalse();
        (await countAsync(conn, "SELECT COUNT(*) FROM ra_child")).ShouldBe(1);

        var existing = await new Table("ra_child").FetchExistingAsync(conn);
        existing.ShouldNotBeNull();
        existing.ForeignKeys.ShouldBeEmpty();
    }

    [Fact]
    public async Task a_self_referencing_foreign_key_survives_the_rebuild()
    {
        await using var conn = await openAsync();

        Table nodes(string noteType)
        {
            var table = new Table("ra_node");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<int>("parent_id").ForeignKeyTo("ra_node", "id");
            table.AddColumn("note", noteType);
            return table;
        }

        await applyAsync(conn, nodes("TEXT"));
        await executeAsync(conn, "INSERT INTO ra_node (id, parent_id, note) VALUES (1, NULL, 'root')");
        await executeAsync(conn, "INSERT INTO ra_node (id, parent_id, note) VALUES (2, 1, 'leaf')");

        await applyAsync(conn, nodes("INTEGER"));

        (await countAsync(conn, "SELECT COUNT(*) FROM ra_node")).ShouldBe(2);
        (await tableExistsAsync(conn, "ra_node_new")).ShouldBeFalse();
        (await danglingReferencesAsync(conn)).ShouldBe(0);
        (await countAsync(conn,
            "SELECT COUNT(*) FROM ra_node c INNER JOIN ra_node p ON p.id = c.parent_id")).ShouldBe(1);
    }

    private static Table TicketsTable(string noteType = "TEXT")
    {
        var table = new Table("ra_tickets");
        table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
        table.AddColumn("note", noteType);
        return table;
    }

    [Fact]
    public async Task the_autoincrement_high_water_mark_survives_the_rebuild()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, TicketsTable());

        await executeAsync(conn, "INSERT INTO ra_tickets (note) VALUES ('a'), ('b'), ('c')");
        await executeAsync(conn, "DELETE FROM ra_tickets WHERE id = 3");

        await applyAsync(conn, TicketsTable("INTEGER"));

        (await countAsync(conn, "SELECT seq FROM sqlite_sequence WHERE name = 'ra_tickets'")).ShouldBe(3);
    }

    [Fact]
    public async Task a_rebuilt_autoincrement_table_does_not_hand_back_a_freed_id()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, TicketsTable());

        await executeAsync(conn, "INSERT INTO ra_tickets (note) VALUES ('a'), ('b'), ('c')");
        await executeAsync(conn, "DELETE FROM ra_tickets WHERE id = 3");

        await applyAsync(conn, TicketsTable("INTEGER"));
        await executeAsync(conn, "INSERT INTO ra_tickets (note) VALUES ('d')");

        (await countAsync(conn, "SELECT id FROM ra_tickets WHERE note = 'd'")).ShouldBe(4);
    }

    /// <summary>
    ///     <c>sqlite_sequence</c> is per-database and an unqualified name resolves against <c>temp</c>
    ///     first, so a temp AUTOINCREMENT table anywhere on the connection used to send the whole
    ///     carry-over to the wrong table -- silently, matching nothing, carrying nothing over.
    /// </summary>
    [Fact]
    public async Task the_autoincrement_mark_survives_a_temp_table_shadowing_sqlite_sequence()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, TicketsTable());

        await executeAsync(conn,
            "CREATE TABLE temp.ra_scratch (id INTEGER PRIMARY KEY AUTOINCREMENT, note TEXT)");
        await executeAsync(conn, "INSERT INTO temp.ra_scratch (note) VALUES ('x')");

        await executeAsync(conn, "INSERT INTO ra_tickets (note) VALUES ('a'), ('b'), ('c')");
        await executeAsync(conn, "DELETE FROM ra_tickets WHERE id = 3");

        await applyAsync(conn, TicketsTable("INTEGER"));
        await executeAsync(conn, "INSERT INTO ra_tickets (note) VALUES ('d')");

        (await countAsync(conn, "SELECT id FROM ra_tickets WHERE note = 'd'")).ShouldBe(4);
        (await countAsync(conn, "SELECT seq FROM temp.sqlite_sequence WHERE name = 'ra_scratch'")).ShouldBe(1);
    }

    /// <summary>
    ///     A database running with enforcement off is allowed to hold dangling rows, and
    ///     <c>foreign_key_check</c> reports every one of them rather than only the ones a rebuild
    ///     could have caused. Checking regardless of the original setting refused the migration and
    ///     blamed the rebuild for rows it never touched.
    /// </summary>
    [Fact]
    public async Task a_rebuild_is_allowed_when_enforcement_was_off_and_rows_already_dangled()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, ParentTable(), ChildTable());

        await executeAsync(conn, "PRAGMA foreign_keys = OFF");
        await executeAsync(conn, "INSERT INTO ra_parent (id, note) VALUES (1, 'kept')");
        await executeAsync(conn, "INSERT INTO ra_child (id, parent_id) VALUES (1, 1), (2, 999)");

        await applyAsync(conn, ParentTable("INTEGER"), ChildTable());

        (await countAsync(conn, "SELECT COUNT(*) FROM ra_parent")).ShouldBe(1);
        (await scalarAsync(conn, "SELECT note FROM ra_parent WHERE id = 1")).ShouldBe("kept");
        (await countAsync(conn, "SELECT COUNT(*) FROM ra_child")).ShouldBe(2);

        Convert.ToInt64(await scalarAsync(conn, "PRAGMA foreign_keys")).ShouldBe(0L);
    }

    /// <summary>
    ///     An emptied table has no row of its own in <c>sqlite_sequence</c> after the copy, so the mark
    ///     has to be inserted rather than raised.
    /// </summary>
    [Fact]
    public async Task the_autoincrement_mark_survives_a_rebuild_of_an_emptied_table()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, TicketsTable());

        await executeAsync(conn, "INSERT INTO ra_tickets (note) VALUES ('a'), ('b')");
        await executeAsync(conn, "DELETE FROM ra_tickets");

        await applyAsync(conn, TicketsTable("INTEGER"));
        await executeAsync(conn, "INSERT INTO ra_tickets (note) VALUES ('c')");

        (await countAsync(conn, "SELECT id FROM ra_tickets WHERE note = 'c'")).ShouldBe(3);
    }

    /// <summary>
    ///     A view referring to the rebuilt table makes SQLite's schema reparse fail the
    ///     <c>ALTER TABLE … RENAME</c> outright with <c>error in view …: no such table</c>, because the
    ///     old table has already been dropped by then.
    /// </summary>
    [Fact]
    public async Task a_view_over_the_rebuilt_table_does_not_break_the_rename()
    {
        await using var conn = await openAsync();
        await applyAsync(conn, ParentTable());
        await executeAsync(conn, "INSERT INTO ra_parent (id, note) VALUES (1, 'kept')");
        await executeAsync(conn, "CREATE VIEW ra_parent_view AS SELECT id, note FROM ra_parent");

        await applyAsync(conn, ParentTable("INTEGER"));

        (await countAsync(conn, "SELECT COUNT(*) FROM ra_parent_view")).ShouldBe(1);
    }

    /// <summary>
    ///     The rebuild needs <c>legacy_alter_table</c> on and foreign key enforcement off, and both are
    ///     connection state the caller owns. Whatever they were set to has to come back, not whatever
    ///     the rebuild happens to find convenient.
    /// </summary>
    [Theory]
    [InlineData("legacy_alter_table", false)]
    [InlineData("legacy_alter_table", true)]
    [InlineData("foreign_keys", false)]
    [InlineData("foreign_keys", true)]
    public async Task a_rebuild_gives_back_the_pragmas_it_borrowed(string pragma, bool setting)
    {
        await using var conn = await openAsync();
        await applyAsync(conn, ParentTable());
        await executeAsync(conn, "INSERT INTO ra_parent (id, note) VALUES (1, 'kept')");
        await executeAsync(conn, $"PRAGMA {pragma} = {(setting ? "ON" : "OFF")}");

        await applyAsync(conn, ParentTable("INTEGER"));

        Convert.ToInt64(await scalarAsync(conn, $"PRAGMA {pragma}")).ShouldBe(setting ? 1L : 0L);
    }

    /// <summary>
    ///     A rebuild that fails takes the same borrowed state with it, so the restoration has to survive
    ///     the failure rather than ride on the success path.
    /// </summary>
    [Theory]
    [InlineData("legacy_alter_table", true)]
    [InlineData("foreign_keys", true)]
    public async Task a_refused_rebuild_gives_the_pragmas_back_too(string pragma, bool setting)
    {
        await using var conn = await openAsync();

        var unconstrained = new Table("ra_child");
        unconstrained.AddColumn<int>("id").AsPrimaryKey();
        unconstrained.AddColumn<int>("parent_id");

        await applyAsync(conn, ParentTable(), unconstrained);
        await executeAsync(conn, "INSERT INTO ra_child (id, parent_id) VALUES (1, 99)");
        await executeAsync(conn, $"PRAGMA {pragma} = {(setting ? "ON" : "OFF")}");

        await Should.ThrowAsync<InvalidOperationException>(
            () => applyAsync(conn, ParentTable(), ChildTable()));

        Convert.ToInt64(await scalarAsync(conn, $"PRAGMA {pragma}")).ShouldBe(setting ? 1L : 0L);
    }

    /// <summary>
    ///     A logger that chooses not to rethrow turns a failed statement into a logged one and lets the
    ///     migration carry on, which is coherent while every statement autocommits on its own. Inside the
    ///     rebuild's transaction "carry on" means reaching <c>COMMIT</c> with half a rebuild applied, so
    ///     the rebuild path notifies the logger and then fails anyway.
    /// </summary>
    private sealed class SwallowingLogger: IMigrationLogger
    {
        public List<Exception> Failures { get; } = new();

        public void SchemaChange(string sql)
        {
        }

        public void OnFailure(DbCommand command, Exception ex)
        {
            Failures.Add(ex);
        }
    }

    private static async Task applyAsync(SqliteConnection conn, IMigrationLogger logger, params Table[] tables)
    {
        var migration = await SchemaMigration.DetermineAsync(conn, CancellationToken.None, tables);
        await new SqliteMigrator().ApplyAllAsync(conn, migration, AutoCreate.All, logger);
    }

    private static Table DuplicateNoteTable()
    {
        var table = new Table("ra_dup");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("note");
        return table;
    }

    private static Table RepointedDuplicateNoteTable()
    {
        var table = new Table("ra_dup");
        table.AddColumn<int>("id");
        table.AddColumn<string>("note").AsPrimaryKey();
        return table;
    }

    private async Task<(SwallowingLogger Logger, Exception? Thrown)> failingRebuildAsync(SqliteConnection conn)
    {
        await applyAsync(conn, DuplicateNoteTable());
        await executeAsync(conn, "INSERT INTO ra_dup (id, note) VALUES (1, 'same'), (2, 'same')");

        var logger = new SwallowingLogger();
        Exception? thrown = null;

        try
        {
            await applyAsync(conn, logger, RepointedDuplicateNoteTable());
        }
        catch (Exception e)
        {
            thrown = e;
        }

        return (logger, thrown);
    }

    [Fact]
    public async Task a_swallowing_logger_still_sees_the_failure()
    {
        await using var conn = await openAsync();

        var (logger, _) = await failingRebuildAsync(conn);

        logger.Failures.ShouldHaveSingleItem();
    }

    [Fact]
    public async Task a_swallowing_logger_cannot_turn_a_failed_rebuild_into_a_silent_one()
    {
        await using var conn = await openAsync();

        var (_, thrown) = await failingRebuildAsync(conn);

        thrown.ShouldNotBeNull();
    }

    [Fact]
    public async Task a_failed_rebuild_rolls_back_even_for_a_swallowing_logger()
    {
        await using var conn = await openAsync();

        await failingRebuildAsync(conn);

        (await tableExistsAsync(conn, "ra_dup_new")).ShouldBeFalse();
        (await countAsync(conn, "SELECT COUNT(*) FROM ra_dup")).ShouldBe(2);

        var existing = await new Table("ra_dup").FetchExistingAsync(conn);
        existing.ShouldNotBeNull();
        existing.PrimaryKeyColumns.ShouldBe(["id"]);
    }

    [Fact]
    public async Task partial_and_expression_indexes_come_back_after_the_rebuild()
    {
        await using var conn = await openAsync();

        Table withIndexes(string noteType)
        {
            var table = ParentTable(noteType);
            table.Indexes.Add(new IndexDefinition("ra_parent_partial") { Columns = ["note"], Predicate = "id > 10" });
            table.Indexes.Add(new IndexDefinition("ra_parent_expression").WithExpression("(lower(note))"));
            return table;
        }

        await applyAsync(conn, withIndexes("TEXT"));
        await applyAsync(conn, withIndexes("INTEGER"));

        (await countAsync(conn,
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name IN ('ra_parent_partial', 'ra_parent_expression')"))
            .ShouldBe(2);

        var migration = await SchemaMigration.DetermineAsync(conn, CancellationToken.None, withIndexes("INTEGER"));
        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_column_can_be_dropped_and_another_retyped_in_one_migration()
    {
        await using var conn = await openAsync();

        var original = new Table("ra_wide");
        original.AddColumn<int>("id").AsPrimaryKey();
        original.AddColumn<string>("keep");
        original.AddColumn<string>("drop_me");
        await applyAsync(conn, original);

        await executeAsync(conn, "INSERT INTO ra_wide (id, keep, drop_me) VALUES (1, 'here', 'gone')");

        var narrowed = new Table("ra_wide");
        narrowed.AddColumn<int>("id").AsPrimaryKey();
        narrowed.AddColumn("keep", "INTEGER");
        await applyAsync(conn, narrowed);

        (await scalarAsync(conn, "SELECT keep FROM ra_wide WHERE id = 1")).ShouldBe("here");

        var migration = await SchemaMigration.DetermineAsync(conn, CancellationToken.None, narrowed);
        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
