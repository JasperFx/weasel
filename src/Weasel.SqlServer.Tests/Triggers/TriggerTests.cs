using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Triggers;
using Xunit;

namespace Weasel.SqlServer.Tests.Triggers;

/// <summary>
///     SQL Server trigger support (weasel#452). SQL Server is the provider with no row-level
///     triggers and no BEFORE, so the interesting cases here are the two refusals.
/// </summary>
[Collection("integration")]
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

    private static Trigger NewTrigger(string note = "touched") => new(
        "triggers.stamp_note",
        "triggers.orders",
        $"UPDATE triggers.orders SET note = '{note}' FROM triggers.orders o INNER JOIN inserted i ON o.id = i.id")
    {
        Timing = TriggerTiming.After, Events = TriggerEvents.Insert
    };

    [Fact]
    public void a_before_trigger_is_refused_with_the_alternative_named()
    {
        var trigger = NewTrigger();
        trigger.Timing = TriggerTiming.Before;

        var ex = Should.Throw<NotSupportedException>(() => trigger.CreateStatement());
        ex.Message.ShouldContain("INSTEAD OF");
    }

    [Fact]
    public void a_when_condition_is_refused_rather_than_dropped()
    {
        var trigger = NewTrigger();
        trigger.Condition = "1 = 1";

        Should.Throw<NotSupportedException>(() => trigger.CreateStatement());
    }

    /// <summary>
    ///     SQL Server triggers are statement-level, so <c>ForEachRow</c> has nothing to emit — the
    ///     trigger sees affected rows through the <c>inserted</c> and <c>deleted</c> tables instead.
    /// </summary>
    [Fact]
    public void for_each_row_is_not_emitted()
    {
        NewTrigger().CreateStatement().ShouldNotContain("FOR EACH");
    }

    [Fact]
    public void several_events_render_as_a_comma_list()
    {
        var trigger = NewTrigger();
        trigger.Events = TriggerEvents.Delete | TriggerEvents.Insert;

        trigger.CreateStatement().ShouldContain("INSERT, DELETE");
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

    [Fact]
    public async Task the_trigger_actually_fires()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await NewTrigger().ApplyChangesAsync(theConnection);

        await theConnection.CreateCommand("insert into triggers.orders (id) values (1)").ExecuteNonQueryAsync();

        var note = await theConnection.CreateCommand("select note from triggers.orders where id = 1")
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
