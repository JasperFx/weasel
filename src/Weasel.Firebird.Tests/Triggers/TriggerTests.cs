using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Triggers;
using Xunit;

namespace Weasel.Firebird.Tests.Triggers;

public class TriggerTests
{
    private static Trigger trigger(string body = "NEW.note = 'touched'")
        => new("trg_stamp_note", "trg_orders", body) { Timing = TriggerTiming.Before, Events = TriggerEvents.Insert };

    [Fact]
    public void the_create_statement_is_create_or_alter_active()
    {
        trigger().CreateStatement().ShouldBe(
            "CREATE OR ALTER TRIGGER trg_stamp_note FOR trg_orders ACTIVE BEFORE INSERT AS\nBEGIN NEW.note = 'touched'; END");
    }

    [Fact]
    public void the_create_statement_is_one_psql_statement()
    {
        var writer = new StringWriter();
        trigger().WriteCreateStatement(new FirebirdMigrator(), writer);

        writer.ToString().ShouldStartWith("SET TERM ^ ;");
        FirebirdScript.Split(writer.ToString()).Single().ShouldStartWith("CREATE OR ALTER TRIGGER");
    }

    [Fact]
    public void several_events_render_as_an_or_list_in_a_fixed_order()
    {
        var subject = trigger();
        subject.Timing = TriggerTiming.After;
        subject.Events = TriggerEvents.Delete | TriggerEvents.Insert | TriggerEvents.Update;

        subject.CreateStatement().ShouldContain("ACTIVE AFTER INSERT OR UPDATE OR DELETE AS");
    }

    [Theory]
    [InlineData("BEGIN NEW.note = 'x'; END", "BEGIN NEW.note = 'x'; END")]
    [InlineData("BEGIN NEW.note = 'x'; END;", "BEGIN NEW.note = 'x'; END")]
    [InlineData("  NEW.note = 'x';  ", "BEGIN NEW.note = 'x'; END")]
    [InlineData("DECLARE VARIABLE n INTEGER;\nBEGIN n = 1; END", "DECLARE VARIABLE n INTEGER;\nBEGIN n = 1; END")]
    [InlineData("-- stamp it\nBEGIN NEW.note = 'x'; END", "-- stamp it\nBEGIN NEW.note = 'x'; END")]
    public void a_body_that_is_not_a_block_is_wrapped_in_one(string body, string written)
    {
        trigger(body).ActionBody().ShouldBe(written);
    }

    [Fact]
    public void a_when_condition_is_refused_rather_than_dropped()
    {
        var subject = trigger();
        subject.Condition = "NEW.id > 0";

        Should.Throw<NotSupportedException>(() => subject.CreateStatement());
    }

    [Fact]
    public void instead_of_is_refused()
    {
        var subject = trigger();
        subject.Timing = TriggerTiming.InsteadOf;

        Should.Throw<NotSupportedException>(() => subject.CreateStatement()).Message.ShouldContain("BEFORE trigger on a view");
    }

    [Fact]
    public void truncate_is_refused()
    {
        var subject = trigger();
        subject.Events = TriggerEvents.Insert | TriggerEvents.Truncate;

        Should.Throw<NotSupportedException>(() => subject.CreateStatement());
    }

    [Fact]
    public void no_events_is_refused()
    {
        var subject = trigger();
        subject.Events = TriggerEvents.None;

        Should.Throw<InvalidOperationException>(() => subject.CreateStatement());
    }

    [Fact]
    public void a_trigger_on_a_table_in_another_schema_is_refused()
    {
        var subject = new Trigger("trg_stamp_note", "sales.trg_orders", "BEGIN END");

        Should.Throw<NotSupportedException>(() => subject.WriteCreateStatement(new FirebirdMigrator(), new StringWriter()));
    }

    [Fact]
    public void the_drop_is_guarded_by_the_trigger_existing()
    {
        var writer = new StringWriter();
        trigger().WriteDropStatement(new FirebirdMigrator(), writer);

        var statement = FirebirdScript.Split(writer.ToString()).Single();
        statement.ShouldContain("RDB$TRIGGER_NAME = 'TRG_STAMP_NOTE'");
        statement.ShouldContain("EXECUTE STATEMENT 'DROP TRIGGER trg_stamp_note'");
    }

    /// <summary>
    ///     The values Firebird 3, 4 and 5 store, read off the catalog. Events written in another order
    ///     pack differently -- <c>UPDATE OR INSERT</c> is 11, <c>INSERT OR UPDATE</c> 17 -- and are the
    ///     same trigger.
    /// </summary>
    [Theory]
    [InlineData(1, TriggerTiming.Before, TriggerEvents.Insert)]
    [InlineData(2, TriggerTiming.After, TriggerEvents.Insert)]
    [InlineData(3, TriggerTiming.Before, TriggerEvents.Update)]
    [InlineData(4, TriggerTiming.After, TriggerEvents.Update)]
    [InlineData(5, TriggerTiming.Before, TriggerEvents.Delete)]
    [InlineData(6, TriggerTiming.After, TriggerEvents.Delete)]
    [InlineData(11, TriggerTiming.Before, TriggerEvents.Insert | TriggerEvents.Update)]
    [InlineData(17, TriggerTiming.Before, TriggerEvents.Insert | TriggerEvents.Update)]
    [InlineData(25, TriggerTiming.Before, TriggerEvents.Insert | TriggerEvents.Delete)]
    [InlineData(28, TriggerTiming.After, TriggerEvents.Update | TriggerEvents.Delete)]
    [InlineData(113, TriggerTiming.Before, TriggerEvents.Insert | TriggerEvents.Update | TriggerEvents.Delete)]
    [InlineData(114, TriggerTiming.After, TriggerEvents.Insert | TriggerEvents.Update | TriggerEvents.Delete)]
    public void the_trigger_type_decodes_to_timing_and_events(long type, TriggerTiming timing, TriggerEvents events)
    {
        Trigger.Decode(type).ShouldBe((timing, events));
    }

    [Theory]
    [InlineData(8192)]
    [InlineData(16385)]
    public void a_database_or_ddl_trigger_decodes_to_no_events(long type)
    {
        Trigger.Decode(type).Events.ShouldBe(TriggerEvents.None);
    }
}
