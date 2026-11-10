using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Triggers;
using Xunit;

namespace Weasel.SqlServer.Tests.Triggers;

[Collection("integration")]
public class trigger_enabled_state: IntegrationContext
{
    public trigger_enabled_state(): base("trg_state")
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await ResetSchema();
        var table = new Table("trg_state.orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("note").AllowNulls();
        await table.ApplyChangesAsync(theConnection);
    }

    private static Trigger model(bool? disabled = null, string note = "touched") =>
        new("trg_state.stamp", "trg_state.orders",
            $"UPDATE trg_state.orders SET note = '{note}' FROM trg_state.orders o INNER JOIN inserted i ON o.id = i.id")
        {
            Timing = TriggerTiming.After, Events = TriggerEvents.Insert, IsDisabled = disabled
        };

    private async Task createInDatabase(bool disabled)
    {
        await model().ApplyChangesAsync(theConnection);
        if (disabled)
        {
            await theConnection.CreateCommand("DISABLE TRIGGER trg_state.stamp ON trg_state.orders").ExecuteNonQueryAsync();
        }
    }

    private async Task<bool> catalogIsDisabled()
        => (bool)(await theConnection.CreateCommand(
                "select is_disabled from sys.triggers where object_id = object_id('trg_state.stamp')")
            .ExecuteScalarAsync())!;

    private async Task<SchemaPatchDifference> difference(Trigger trigger)
        => (await trigger.FindDeltaAsync(theConnection)).Difference;

    private static string createSql(Trigger trigger)
    {
        var writer = new StringWriter();
        trigger.WriteCreateStatement(new SqlServerMigrator(), writer);
        return writer.ToString();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task the_enabled_state_is_read_back(bool disabled)
    {
        await createInDatabase(disabled);

        (await model().FetchExistingIsDisabledAsync(theConnection)).ShouldBe(disabled);
    }

    [Fact]
    public async Task a_missing_trigger_reads_back_no_state()
    {
        (await model().FetchExistingIsDisabledAsync(theConnection)).ShouldBeNull();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task an_unpinned_model_is_unchanged_whatever_the_state(bool disabled)
    {
        await createInDatabase(disabled);

        (await difference(model())).ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void an_unpinned_trigger_renders_as_before()
    {
        createSql(model()).ShouldNotContain("DISABLE TRIGGER");
        createSql(model(disabled: false)).ShouldBe(createSql(model()));
    }

    [Fact]
    public void a_disabled_pin_renders_the_disable()
    {
        createSql(model(disabled: true)).ShouldEndWith(
            $"DISABLE TRIGGER trg_state.stamp ON trg_state.orders;{Environment.NewLine}");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task a_pin_matching_the_database_is_unchanged(bool disabled)
    {
        await createInDatabase(disabled);

        (await difference(model(disabled))).ShouldBe(SchemaPatchDifference.None);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task a_pin_the_database_differs_from_is_drift_that_converges(bool disabled)
    {
        await createInDatabase(!disabled);
        var pinned = model(disabled);

        (await difference(pinned)).ShouldBe(SchemaPatchDifference.Update);

        await pinned.ApplyChangesAsync(theConnection);

        (await catalogIsDisabled()).ShouldBe(disabled);
        (await difference(pinned)).ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_trigger_created_with_a_disabled_pin_is_created_disabled()
    {
        var pinned = model(disabled: true);
        (await difference(pinned)).ShouldBe(SchemaPatchDifference.Create);

        await pinned.ApplyChangesAsync(theConnection);

        (await catalogIsDisabled()).ShouldBeTrue();
        (await difference(pinned)).ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_changed_body_is_still_reported_with_a_matching_pin()
    {
        await createInDatabase(disabled: true);

        (await difference(model(disabled: true, note: "changed"))).ShouldBe(SchemaPatchDifference.Update);
    }
}
