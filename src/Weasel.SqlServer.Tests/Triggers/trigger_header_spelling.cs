using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Triggers;
using Xunit;

namespace Weasel.SqlServer.Tests.Triggers;

[Collection("integration")]
public class trigger_header_spelling: IntegrationContext
{
    private const string Body = "UPDATE trg_spelling.orders SET note = 'touched' FROM trg_spelling.orders o INNER JOIN inserted i ON o.id = i.id";

    public trigger_header_spelling(): base("trg_spelling")
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await ResetSchema();
        await table("orders").ApplyChangesAsync(theConnection);
        await table("archive").ApplyChangesAsync(theConnection);
    }

    private static Table table(string name)
    {
        var table = new Table($"trg_spelling.{name}");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("note").AllowNulls();
        return table;
    }

    private static Trigger model(
        bool ignoreHeaderSpelling,
        TriggerEvents events = TriggerEvents.Insert | TriggerEvents.Update,
        TriggerTiming timing = TriggerTiming.After,
        string target = "trg_spelling.orders",
        string body = Body) =>
        new("trg_spelling.stamp", target, body)
        {
            Timing = timing, Events = events, IgnoreHeaderSpelling = ignoreHeaderSpelling
        };

    private async Task createFromSource(string header, string body = Body)
        => await theConnection.CreateCommand(header + "\n" + body).ExecuteNonQueryAsync();

    private async Task<SchemaPatchDifference> difference(Trigger trigger)
        => (await trigger.FindDeltaAsync(theConnection)).Difference;

    public static TheoryData<string> SourceSpellings => new()
    {
        "CREATE TRIGGER [trg_spelling].[stamp] ON [trg_spelling].[orders] AFTER INSERT, UPDATE AS",
        "create trigger trg_spelling.stamp on trg_spelling.orders for insert, update as",
        "CREATE TRIGGER [trg_spelling].[stamp] ON [trg_spelling].[orders] FOR UPDATE,INSERT AS",
        "-- maintains note\n/* written by hand */\nCREATE TRIGGER [trg_spelling].[stamp]\n    ON [trg_spelling].[orders]\n    AFTER INSERT, UPDATE\nAS",
        "CREATE TRIGGER \"trg_spelling\".\"stamp\" ON \"trg_spelling\".\"orders\" AFTER INSERT, UPDATE AS",
        "CREATE OR ALTER TRIGGER [trg_spelling].[stamp] ON [trg_spelling].[orders] AFTER INSERT, UPDATE AS"
    };

    [Theory]
    [MemberData(nameof(SourceSpellings))]
    public async Task the_source_spelling_is_unchanged_when_opted_in(string header)
    {
        await createFromSource(header);

        (await difference(model(ignoreHeaderSpelling: true))).ShouldBe(SchemaPatchDifference.None);
    }

    [Theory]
    [MemberData(nameof(SourceSpellings))]
    public async Task the_source_spelling_is_still_drift_by_default(string header)
    {
        await createFromSource(header);

        (await difference(model(ignoreHeaderSpelling: false))).ShouldBe(SchemaPatchDifference.Update);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task a_trigger_weasel_created_is_unchanged_either_way(bool ignoreHeaderSpelling)
    {
        await model(ignoreHeaderSpelling).ApplyChangesAsync(theConnection);

        (await difference(model(ignoreHeaderSpelling))).ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_missing_trigger_is_still_created()
    {
        (await difference(model(ignoreHeaderSpelling: true))).ShouldBe(SchemaPatchDifference.Create);
    }

    private const string SourceHeader =
        "CREATE TRIGGER [trg_spelling].[stamp] ON [trg_spelling].[orders] FOR INSERT, UPDATE AS";

    [Fact]
    public async Task a_changed_body_is_still_reported()
    {
        await createFromSource(SourceHeader);

        (await difference(model(true, body: Body.Replace("touched", "changed")))).ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task changed_events_are_still_reported()
    {
        await createFromSource(SourceHeader);

        (await difference(model(true, events: TriggerEvents.Insert))).ShouldBe(SchemaPatchDifference.Update);
        (await difference(model(true, events: TriggerEvents.Insert | TriggerEvents.Update | TriggerEvents.Delete)))
            .ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task a_changed_timing_is_still_reported()
    {
        await createFromSource(SourceHeader);

        (await difference(model(true, timing: TriggerTiming.InsteadOf))).ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task a_changed_target_is_still_reported()
    {
        await createFromSource(SourceHeader);

        (await difference(model(true, target: "trg_spelling.archive"))).ShouldBe(SchemaPatchDifference.Update);
    }

    [Theory]
    [InlineData("CREATE TRIGGER [trg_spelling].[stamp] ON [trg_spelling].[orders] FOR INSERT, UPDATE NOT FOR REPLICATION AS")]
    [InlineData("CREATE TRIGGER [trg_spelling].[stamp] ON [trg_spelling].[orders] WITH EXECUTE AS OWNER FOR INSERT, UPDATE AS")]
    public async Task a_header_option_the_model_cannot_express_is_still_reported(string header)
    {
        await createFromSource(header);

        (await difference(model(ignoreHeaderSpelling: true))).ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task applying_a_real_change_converges_when_opted_in()
    {
        await createFromSource(SourceHeader);
        var changed = model(true, events: TriggerEvents.Insert);

        await changed.ApplyChangesAsync(theConnection);

        (await difference(changed)).ShouldBe(SchemaPatchDifference.None);
    }
}
