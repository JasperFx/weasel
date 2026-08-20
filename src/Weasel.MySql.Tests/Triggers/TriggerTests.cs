using MySqlConnector;
using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Weasel.MySql.Triggers;
using Xunit;

namespace Weasel.MySql.Tests.Triggers;

/// <summary>
///     MySQL trigger support (weasel#452). MySQL stores the action statement verbatim — unlike a
///     view definition, which it rewrites — so the delta is a straight comparison of the body.
/// </summary>
/// <remarks>
///     Root credentials, and not only for the usual schema-permission reason: creating a trigger
///     needs the TRIGGER privilege, and on a server with binary logging enabled it also needs SUPER
///     or <c>log_bin_trust_function_creators</c>. MySQL refuses otherwise with a message about the
///     SUPER privilege that never mentions triggers.
/// </remarks>
[Collection("integration")]
public class TriggerTests: IAsyncLifetime
{
    private const string SchemaName = "weasel_testing";

    private MySqlConnection theConnection = default!;

    public async ValueTask InitializeAsync()
    {
        var builder = new MySqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            UserID = "root", Password = "P@55w0rd", Database = SchemaName
        };

        theConnection = new MySqlConnection(builder.ConnectionString);
        await theConnection.OpenAsync();

        await theConnection.CreateCommand($"DROP TRIGGER IF EXISTS `{SchemaName}`.trg_stamp_note")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand($"DROP TABLE IF EXISTS `{SchemaName}`.trg_orders")
            .ExecuteNonQueryAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await theConnection.CreateCommand($"DROP TRIGGER IF EXISTS `{SchemaName}`.trg_stamp_note")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand($"DROP TABLE IF EXISTS `{SchemaName}`.trg_orders")
            .ExecuteNonQueryAsync();
        await theConnection.CloseAsync();
        await theConnection.DisposeAsync();
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
        $"SET NEW.note = '{note}'")
    {
        Timing = TriggerTiming.Before, Events = TriggerEvents.Insert
    };

    [Fact]
    public void more_than_one_event_is_refused_rather_than_narrowed()
    {
        var trigger = NewTrigger();
        trigger.Events = TriggerEvents.Insert | TriggerEvents.Update;

        var ex = Should.Throw<InvalidOperationException>(() => trigger.CreateStatement());
        ex.Message.ShouldContain("exactly one event");
    }

    [Fact]
    public void a_when_condition_is_refused_rather_than_dropped()
    {
        var trigger = NewTrigger();
        trigger.Condition = "NEW.id > 0";

        Should.Throw<NotSupportedException>(() => trigger.CreateStatement());
    }

    [Fact]
    public async Task a_trigger_round_trips_and_reports_no_delta()
    {
        await SourceTable().ApplyChangesAsync(theConnection);

        var trigger = NewTrigger();
        await trigger.ApplyChangesAsync(theConnection);

        (await trigger.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await trigger.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_trigger_actually_fires()
    {
        await SourceTable().ApplyChangesAsync(theConnection);
        await NewTrigger().ApplyChangesAsync(theConnection);

        await theConnection.CreateCommand($"INSERT INTO `{SchemaName}`.trg_orders (id) VALUES (1)")
            .ExecuteNonQueryAsync();

        var note = await theConnection.CreateCommand($"SELECT note FROM `{SchemaName}`.trg_orders WHERE id = 1")
            .ExecuteScalarAsync();

        note.ShouldBe("touched");
    }

    /// <summary>
    ///     A multi-statement body, which is the case that made MySQL's migrator stop splitting delta
    ///     SQL on semicolons — the split shredded every <c>BEGIN … END</c> block it saw.
    /// </summary>
    [Fact]
    public async Task a_begin_end_body_with_semicolons_survives_execution()
    {
        await SourceTable().ApplyChangesAsync(theConnection);

        var trigger = new Trigger(
            $"{SchemaName}.trg_stamp_note",
            $"{SchemaName}.trg_orders",
            "BEGIN SET NEW.note = 'first'; SET NEW.note = CONCAT(NEW.note, '-second'); END")
        {
            Timing = TriggerTiming.Before, Events = TriggerEvents.Insert
        };

        await trigger.ApplyChangesAsync(theConnection);

        await theConnection.CreateCommand($"INSERT INTO `{SchemaName}`.trg_orders (id) VALUES (2)")
            .ExecuteNonQueryAsync();

        var note = await theConnection.CreateCommand($"SELECT note FROM `{SchemaName}`.trg_orders WHERE id = 2")
            .ExecuteScalarAsync();

        note.ShouldBe("first-second");
    }

    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        await SourceTable().ApplyChangesAsync(theConnection);
        await NewTrigger().ApplyChangesAsync(theConnection);

        var changed = NewTrigger("changed");

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await changed.ApplyChangesAsync(theConnection);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
