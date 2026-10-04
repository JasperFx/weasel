using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Weasel.Firebird.Triggers;
using Weasel.Firebird.Views;
using Xunit;

namespace Weasel.Firebird.Tests.Triggers;

/// <summary>
///     Firebird keeps a trigger's body as source and the rest -- its table, when it fires, whether it is
///     active -- in <c>RDB$TRIGGERS</c>, so each of those is a change a delta has to see.
/// </summary>
public class triggers_in_the_database: IntegrationContext
{
    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();

        await ApplyAsync(sourceTable("trg_orders"), sourceTable("trg_archive"));
    }

    private static Table sourceTable(string name)
    {
        var table = new Table(name);
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("note", "VARCHAR(40)");
        return table;
    }

    private static Trigger stamp(string note = "touched", string target = "trg_orders")
        => new("trg_stamp_note", target, $"NEW.note = '{note}'")
        {
            Timing = TriggerTiming.Before, Events = TriggerEvents.Insert
        };

    private async Task<string?> noteAsync(string table, int id, string column = "note", string key = "id")
    {
        await using var cmd = theConnection.CreateCommand($"SELECT {column} FROM {table} WHERE {key} = {id}");
        return await cmd.ExecuteScalarAsync() as string;
    }

    [Fact]
    public async Task a_missing_trigger_reports_create_and_applying_it_converges()
    {
        var trigger = stamp();

        (await trigger.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await trigger.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Create);

        await ApplyAsync(trigger);

        (await trigger.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await DetermineAsync(stamp())).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_trigger_actually_fires()
    {
        await ApplyAsync(stamp());

        await ExecuteAsync("INSERT INTO trg_orders (id) VALUES (1)");

        (await noteAsync("trg_orders", 1)).ShouldBe("touched");
    }

    /// <summary>
    ///     A block full of semicolons, declarations and a literal with a semicolon in it: the reason the
    ///     trigger is written inside <c>SET TERM</c>.
    /// </summary>
    [Fact]
    public async Task a_block_with_declarations_and_semicolons_survives_and_reads_back_unchanged()
    {
        var trigger = new Trigger("trg_stamp_note", "trg_orders", """
            DECLARE VARIABLE suffix VARCHAR(10) = '-second';
            BEGIN
                -- first, then second
                NEW.note = 'first;';
                NEW.note = NEW.note || suffix;
            END
            """)
        {
            Timing = TriggerTiming.Before, Events = TriggerEvents.Insert | TriggerEvents.Update
        };

        await ApplyAsync(trigger);
        await ExecuteAsync("INSERT INTO trg_orders (id) VALUES (2)");

        (await noteAsync("trg_orders", 2)).ShouldBe("first;-second");

        var delta = await trigger.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None, delta.ToString());
    }

    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        await ApplyAsync(stamp());

        var changed = stamp("changed");
        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(changed);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        await ExecuteAsync("INSERT INTO trg_orders (id) VALUES (3)");
        (await noteAsync("trg_orders", 3)).ShouldBe("changed");
    }

    [Fact]
    public async Task changed_timing_or_events_report_update_and_applying_them_converges()
    {
        await ApplyAsync(stamp());

        var changed = stamp();
        changed.Events = TriggerEvents.Insert | TriggerEvents.Update;
        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(changed);
        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        var after = new Trigger("trg_stamp_note", "trg_orders", "BEGIN END")
        {
            Timing = TriggerTiming.After, Events = TriggerEvents.Delete
        };
        (await after.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(after);
        (await after.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Events written in another order pack into another <c>RDB$TRIGGER_TYPE</c>, and are the same
    ///     trigger.
    /// </summary>
    [Fact]
    public async Task events_written_in_another_order_are_not_a_change()
    {
        await ExecuteAsync("""
            SET TERM ^ ;
            CREATE TRIGGER trg_stamp_note FOR trg_orders ACTIVE BEFORE UPDATE OR INSERT AS
            BEGIN NEW.note = 'touched'; END
            ^
            SET TERM ; ^
            """);

        var model = stamp();
        model.Events = TriggerEvents.Insert | TriggerEvents.Update;

        (await model.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_trigger_moved_to_another_table_reports_update_and_applying_it_converges()
    {
        await ApplyAsync(stamp());

        var moved = stamp(target: "trg_archive");
        (await moved.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(moved);

        (await moved.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        await ExecuteAsync("INSERT INTO trg_orders (id) VALUES (4)");
        await ExecuteAsync("INSERT INTO trg_archive (id) VALUES (4)");
        (await noteAsync("trg_orders", 4)).ShouldBeNull();
        (await noteAsync("trg_archive", 4)).ShouldBe("touched");
    }

    /// <summary>
    ///     A trigger switched off by hand is a trigger that does nothing. <c>CREATE OR ALTER</c> without
    ///     <c>ACTIVE</c> would leave it off, which is why the statement says <c>ACTIVE</c>.
    /// </summary>
    [Fact]
    public async Task an_inactive_trigger_reports_update_and_applying_it_reactivates_it()
    {
        await ApplyAsync(stamp());
        await ExecuteAsync("ALTER TRIGGER trg_stamp_note INACTIVE");

        (await stamp().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(stamp());

        (await stamp().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        await ExecuteAsync("INSERT INTO trg_orders (id) VALUES (5)");
        (await noteAsync("trg_orders", 5)).ShouldBe("touched");
    }

    /// <summary>
    ///     Firebird has no <c>INSTEAD OF</c>; a <c>BEFORE</c> trigger on a view does its work.
    /// </summary>
    [Fact]
    public async Task a_trigger_on_a_view_makes_it_writable()
    {
        await ApplyAsync(new View("trg_order_notes", "select o.id, o.note from trg_orders o join trg_archive a on a.id = o.id"));

        var trigger = new Trigger("trg_order_notes_insert", "trg_order_notes", """
            BEGIN
                INSERT INTO trg_orders (id, note) VALUES (NEW.id, NEW.note);
                INSERT INTO trg_archive (id, note) VALUES (NEW.id, NEW.note);
            END
            """)
        {
            Timing = TriggerTiming.Before, Events = TriggerEvents.Insert
        };

        await ApplyAsync(trigger);
        await ExecuteAsync("INSERT INTO trg_order_notes (id, note) VALUES (6, 'through the view')");

        (await noteAsync("trg_orders", 6)).ShouldBe("through the view");
        (await noteAsync("trg_archive", 6)).ShouldBe("through the view");
        (await trigger.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     An EF Core model's tables keep the case of their names. The trigger names its table the way
    ///     the table names itself, and a trigger read back from the catalog names it exactly too, so a
    ///     rollback lands on the same table.
    /// </summary>
    [Fact]
    public async Task a_trigger_on_a_case_preserved_table_is_created_fires_and_reads_back_unchanged()
    {
        var blogs = new Table("Blogs") { PreserveIdentifierCase = true };
        blogs.AddColumn<int>("Id").AsPrimaryKey();
        blogs.AddColumn("Title", "VARCHAR(40)");
        await ApplyAsync(blogs);

        Trigger stampTitle(string title) => new("trg_blogs_title", blogs, $"NEW.\"Title\" = '{title}'")
        {
            Events = TriggerEvents.Insert
        };

        var trigger = stampTitle("stamped");
        await ApplyAsync(trigger);

        (await trigger.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await DetermineAsync(stampTitle("stamped"))).Difference.ShouldBe(SchemaPatchDifference.None);
        (await new Trigger("trg_blogs_title", "Blogs", "NEW.\"Title\" = 'stamped'") { PreserveTargetCase = true }
            .FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        await ExecuteAsync("INSERT INTO \"Blogs\" (\"Id\") VALUES (1)");
        (await noteAsync("\"Blogs\"", 1, "\"Title\"", "\"Id\"")).ShouldBe("stamped");

        // Folded, the name is BLOGS: another table, so a trigger on it is a change.
        (await new Trigger("trg_blogs_title", "Blogs", "NEW.\"Title\" = 'stamped'").FindDeltaAsync(theConnection))
            .Difference.ShouldBe(SchemaPatchDifference.Update);

        var migration = await ApplyAsync(stampTitle("changed"));
        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await trigger.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task rolling_back_an_update_restores_the_previous_trigger()
    {
        await ApplyAsync(stamp());

        var migration = await ApplyAsync(stamp("changed", "trg_archive"));
        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await stamp().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task dropping_the_schema_takes_its_triggers_with_it()
    {
        await ApplyAsync(stamp());
        await ApplyAsync(new View("trg_view", "select id, note from trg_orders"));
        var onView = new Trigger("trg_on_view", "trg_view", "BEGIN END") { Events = TriggerEvents.Delete };
        await ApplyAsync(onView);

        await theConnection.DropSchemaAsync();

        (await stamp().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await onView.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    [Fact]
    public async Task the_drop_statement_drops_it_and_runs_again_harmlessly()
    {
        await ApplyAsync(stamp());

        await stamp().DropAsync(theConnection);
        await stamp().DropAsync(theConnection);

        (await stamp().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
