using Microsoft.Data.Sqlite;
using Shouldly;
using JasperFx;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Weasel.Sqlite.Triggers;
using Xunit;

namespace Weasel.Sqlite.Tests.Triggers;

/// <summary>
///     SQLite trigger support (weasel#452), plus the affordance the design decision called out as
///     required from day one: SQLite rebuilds a table to change most things about it, and
///     <c>DROP TABLE</c> takes the table's triggers with it silently.
/// </summary>
public class TriggerTests
{
    private readonly string _connectionString = $"Data Source={Path.GetTempFileName()};";

    private async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync();
        await conn.ResetSchemaAsync("main");
        return conn;
    }

    private static Table SourceTable()
    {
        var table = new Table("trg_orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("quantity");
        table.AddColumn<string>("note");
        return table;
    }

    /// <summary>
    ///     Run SQLite's table rebuild: create a new table, copy the surviving columns, drop the old
    ///     one, rename, and put the indexes and triggers back.
    /// </summary>
    /// <remarks>
    ///     Driven through <c>TableDelta.WriteUpdate</c> rather than through the migrator on purpose.
    ///     A rebuild reports <see cref="SchemaPatchDifference.Invalid" />, and <c>Migrator</c>
    ///     answers <c>Invalid</c> by dropping and recreating the table — so the migrator never
    ///     reaches this path at all, and takes the table's rows with it when it goes. That is
    ///     weasel#477, a separate bug; the trigger restoration is emitted here and is correct
    ///     wherever the rebuild runs.
    /// </remarks>
    private static async Task rebuildAsync(SqliteConnection conn, Table table)
    {
        var delta = await table.FindDeltaAsync(conn);

        var writer = new StringWriter();
        delta.WriteUpdate(new SqliteMigrator(), writer);

        await conn.CreateCommand(writer.ToString()).ExecuteNonQueryAsync();
    }

    private static Trigger NewTrigger(string body = "UPDATE trg_orders SET note = 'touched' WHERE id = NEW.id")
        => new("trg_orders_after_insert", "trg_orders", body)
        {
            Timing = TriggerTiming.After, Events = TriggerEvents.Insert
        };

    [Fact]
    public void write_create_statement_drops_first_so_it_is_idempotent()
    {
        var writer = new StringWriter();
        NewTrigger().WriteCreateStatement(new SqliteMigrator(), writer);

        var sql = writer.ToString();

        sql.ShouldContain("DROP TRIGGER IF EXISTS");
        sql.ShouldContain("CREATE TRIGGER");
        sql.ShouldContain("AFTER INSERT");
    }

    /// <summary>
    ///     SQLite triggers are always row-level — <c>FOR EACH STATEMENT</c> is a syntax error — so
    ///     the flag is not emitted rather than emitted wrongly.
    /// </summary>
    [Fact]
    public void for_each_row_is_not_emitted()
    {
        var writer = new StringWriter();
        NewTrigger().WriteCreateStatement(new SqliteMigrator(), writer);

        writer.ToString().ShouldNotContain("FOR EACH");
    }

    [Fact]
    public void more_than_one_event_is_refused_rather_than_narrowed()
    {
        var trigger = NewTrigger();
        trigger.Events = TriggerEvents.Insert | TriggerEvents.Update;

        var ex = Should.Throw<InvalidOperationException>(() => trigger.CreateStatement());
        ex.Message.ShouldContain("exactly one event");
    }

    [Fact]
    public async Task a_trigger_round_trips_and_reports_no_delta()
    {
        await using var conn = await openAsync();
        await SourceTable().ApplyChangesAsync(conn);

        var trigger = NewTrigger();
        await trigger.ApplyChangesAsync(conn);

        (await trigger.ExistsInDatabaseAsync(conn)).ShouldBeTrue();
        (await trigger.FindDeltaAsync(conn)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        await using var conn = await openAsync();
        await SourceTable().ApplyChangesAsync(conn);
        await NewTrigger().ApplyChangesAsync(conn);

        var changed = NewTrigger("UPDATE trg_orders SET note = 'changed' WHERE id = NEW.id");

        (await changed.FindDeltaAsync(conn)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await changed.ApplyChangesAsync(conn);

        (await changed.FindDeltaAsync(conn)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_trigger_actually_fires()
    {
        await using var conn = await openAsync();
        await SourceTable().ApplyChangesAsync(conn);
        await NewTrigger().ApplyChangesAsync(conn);

        await conn.CreateCommand("INSERT INTO trg_orders (id, quantity) VALUES (1, 5)").ExecuteNonQueryAsync();

        var note = await conn.CreateCommand("SELECT note FROM trg_orders WHERE id = 1").ExecuteScalarAsync();
        note.ShouldBe("touched");
    }

    /// <summary>
    ///     The hazard the design decision insisted on covering from day one. SQLite rebuilds the
    ///     table to change a column, <c>DROP TABLE</c> silently takes the triggers with it, and the
    ///     resulting schema looks entirely correct while the user's data-integrity logic is gone.
    /// </summary>
    [Fact]
    public async Task a_trigger_survives_the_table_being_rebuilt()
    {
        await using var conn = await openAsync();
        await SourceTable().ApplyChangesAsync(conn);
        await NewTrigger().ApplyChangesAsync(conn);

        // A column *type* change, which SQLite cannot do in place -- ADD COLUMN would take the
        // incremental path and never exercise the rebuild. TableDelta creates a new table, copies,
        // drops the old one and renames.
        var retyped = new Table("trg_orders");
        retyped.AddColumn<int>("id").AsPrimaryKey();
        retyped.AddColumn<string>("quantity");
        retyped.AddColumn<string>("note");

        await rebuildAsync(conn, retyped);

        (await NewTrigger().ExistsInDatabaseAsync(conn))
            .ShouldBeTrue("the table rebuild dropped the trigger and did not put it back");

        // ...and it still works, which is the part that actually matters.
        await conn.CreateCommand("INSERT INTO trg_orders (id, quantity) VALUES (2, 7)").ExecuteNonQueryAsync();

        var note = await conn.CreateCommand("SELECT note FROM trg_orders WHERE id = 2").ExecuteScalarAsync();
        note.ShouldBe("touched");
    }

    /// <summary>
    ///     Including a trigger Weasel never declared. The restoration reads <c>sqlite_master</c>
    ///     rather than the model precisely so that a hand-written trigger is not collateral damage.
    /// </summary>
    [Fact]
    public async Task a_trigger_weasel_did_not_declare_survives_the_rebuild_too()
    {
        await using var conn = await openAsync();
        await SourceTable().ApplyChangesAsync(conn);

        await conn.CreateCommand(
                "CREATE TRIGGER handwritten AFTER INSERT ON trg_orders BEGIN "
                + "UPDATE trg_orders SET note = 'by hand' WHERE id = NEW.id; END")
            .ExecuteNonQueryAsync();

        var retyped = new Table("trg_orders");
        retyped.AddColumn<int>("id").AsPrimaryKey();
        retyped.AddColumn<string>("quantity");
        retyped.AddColumn<string>("note");
        await rebuildAsync(conn, retyped);

        var count = await conn
            .CreateCommand("SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name = 'handwritten'")
            .ExecuteScalarAsync();

        Convert.ToInt32(count).ShouldBe(1);
    }

    [Fact]
    public async Task dropping_the_schema_takes_its_triggers_with_it()
    {
        await using var conn = await openAsync();
        await SourceTable().ApplyChangesAsync(conn);
        await NewTrigger().ApplyChangesAsync(conn);

        await conn.ResetSchemaAsync("main");

        (await NewTrigger().ExistsInDatabaseAsync(conn)).ShouldBeFalse();
    }
}
