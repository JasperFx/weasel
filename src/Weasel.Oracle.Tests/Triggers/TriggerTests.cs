using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Tables;
using Weasel.Oracle.Triggers;
using Xunit;

namespace Weasel.Oracle.Tests.Triggers;

/// <summary>
///     Oracle trigger support (weasel#452). Oracle stores the trigger body verbatim in
///     <c>all_triggers</c>, so the delta is a whitespace-insensitive comparison — with the usual
///     Oracle caveat that the column is a LONG and reads back empty unless the command says
///     otherwise.
/// </summary>
[Collection("integration")]
public class TriggerTests: IntegrationContext
{
    private const string SchemaName = "WEASEL";

    public TriggerTests(): base(SchemaName)
    {
    }

    private static Table SourceTable()
    {
        var table = new Table($"{SchemaName}.trg_orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("note");
        return table;
    }

    private static Trigger NewTrigger(string note = "touched") => new(
        $"{SchemaName}.trg_stamp_note",
        $"{SchemaName}.trg_orders",
        $"BEGIN :NEW.note := '{note}'; END;")
    {
        Timing = TriggerTiming.Before, Events = TriggerEvents.Insert
    };

    [Fact]
    public void the_create_statement_uses_create_or_replace()
    {
        var sql = NewTrigger().CreateStatement();

        sql.ShouldContain("CREATE OR REPLACE TRIGGER");
        sql.ShouldContain("BEFORE INSERT");
        sql.ShouldContain("FOR EACH ROW");
    }

    [Fact]
    public void several_events_render_as_an_or_list()
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

        var trigger = NewTrigger();
        await trigger.ApplyChangesAsync(theConnection);

        (await trigger.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await trigger.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     <c>all_triggers.trigger_body</c> is a LONG, and ODP.NET reads a LONG back as an empty
    ///     string unless the command sets <c>InitialLONGFetchSize</c> — the trap weasel#450 hit on
    ///     <c>all_views.TEXT</c>. Without it a trigger that plainly exists looks absent.
    /// </summary>
    [Fact]
    public async Task the_body_reads_back_rather_than_coming_up_empty()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await NewTrigger().ApplyChangesAsync(theConnection);

        var body = await NewTrigger().FetchExistingBodyAsync(theConnection);

        body.ShouldNotBeNull();
        body!.ShouldContain("touched");
    }

    [Fact]
    public async Task the_trigger_actually_fires()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await NewTrigger().ApplyChangesAsync(theConnection);

        await theConnection.CreateCommand($"INSERT INTO {SchemaName}.trg_orders (id) VALUES (1)")
            .ExecuteNonQueryAsync();

        var note = await theConnection.CreateCommand($"SELECT note FROM {SchemaName}.trg_orders WHERE id = 1")
            .ExecuteScalarAsync();

        note.ShouldBe("touched");
    }

    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await NewTrigger().ApplyChangesAsync(theConnection);

        var changed = NewTrigger("changed");

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await changed.ApplyChangesAsync(theConnection);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Oracle's teardown enumerates object types by hand, and weasel#465 taught it triggers
    ///     before anything could create one. This is the test that arms that.
    /// </summary>
    [Fact]
    public async Task dropping_the_schema_takes_its_triggers_with_it()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await NewTrigger().ApplyChangesAsync(theConnection);

        await ResetSchema();

        (await NewTrigger().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
