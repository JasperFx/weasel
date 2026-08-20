using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Functions;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Triggers;
using Xunit;

namespace Weasel.Postgresql.Tests.Triggers;

/// <summary>
///     PostgreSQL trigger support (weasel#452). PostgreSQL is the provider where a trigger has no
///     body of its own — it names a function to execute — so these also cover the composition with
///     <see cref="Function" /> that makes that work.
/// </summary>
[Collection("triggers")]
public class TriggerTests: IntegrationContext
{
    public TriggerTests(): base("triggers")
    {
    }

    private static Table SourceTable()
    {
        var table = new Table("triggers.orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("note");
        return table;
    }

    private static Function AuditFunction(string note = "touched") => new(
        new PostgresqlObjectName("triggers", "stamp_note"),
        $@"CREATE OR REPLACE FUNCTION triggers.stamp_note() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.note := '{note}';
    RETURN NEW;
END;
$$;");

    private static Trigger NewTrigger() => new("triggers.stamp_note_trigger", "triggers.orders", "triggers.stamp_note()")
    {
        Timing = TriggerTiming.Before, Events = TriggerEvents.Insert
    };

    [Fact]
    public void the_create_statement_executes_a_function_rather_than_a_body()
    {
        var sql = NewTrigger().CreateStatement();

        sql.ShouldContain("EXECUTE FUNCTION triggers.stamp_note()");
        sql.ShouldContain("BEFORE INSERT");
        sql.ShouldContain("FOR EACH ROW");
    }

    [Fact]
    public void several_events_render_as_an_or_list_in_a_stable_order()
    {
        var trigger = NewTrigger();
        trigger.Events = TriggerEvents.Delete | TriggerEvents.Insert;

        trigger.CreateStatement().ShouldContain("INSERT OR DELETE");
    }

    [Fact]
    public async Task a_trigger_round_trips_and_reports_no_delta()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await AuditFunction().ApplyChangesAsync(theConnection);

        var trigger = NewTrigger();
        await trigger.ApplyChangesAsync(theConnection);

        (await trigger.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await trigger.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_trigger_actually_fires()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await AuditFunction().ApplyChangesAsync(theConnection);
        await NewTrigger().ApplyChangesAsync(theConnection);

        await theConnection.CreateCommand("insert into triggers.orders (id) values (1)").ExecuteNonQueryAsync();

        var note = await theConnection.CreateCommand("select note from triggers.orders where id = 1")
            .ExecuteScalarAsync();

        note.ShouldBe("touched");
    }

    [Fact]
    public async Task a_changed_trigger_reports_update_and_applying_it_converges()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await AuditFunction().ApplyChangesAsync(theConnection);
        await NewTrigger().ApplyChangesAsync(theConnection);

        var changed = NewTrigger();
        changed.Events = TriggerEvents.Insert | TriggerEvents.Update;

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await changed.ApplyChangesAsync(theConnection);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task dropping_the_schema_takes_its_triggers_with_it()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await AuditFunction().ApplyChangesAsync(theConnection);
        await NewTrigger().ApplyChangesAsync(theConnection);

        await ResetSchema();

        (await NewTrigger().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
