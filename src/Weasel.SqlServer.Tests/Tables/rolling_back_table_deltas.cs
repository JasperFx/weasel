using Baseline.Dates;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

public class rolling_back_table_deltas: IntegrationContext
{
    private Table initial;
    private Table configured;

    public rolling_back_table_deltas(): base("rollbacks")
    {
        initial = new Table("rollbacks.people");
        initial.AddColumn<int>("id").AsPrimaryKey();
        initial.AddColumn<string>("first_name");
        initial.AddColumn<string>("last_name");
        initial.AddColumn<string>("user_name");
        initial.AddColumn("data", "text");

        configured = new Table("rollbacks.people");
        configured.AddColumn<int>("id").AsPrimaryKey();
        configured.AddColumn<string>("first_name");
        configured.AddColumn<string>("last_name");
        configured.AddColumn<string>("user_name");
        configured.AddColumn("data", "text");
    }

    private async Task AssertRollbackIsSuccessful(params Table[] otherTables)
    {
        await ResetSchema();

        foreach (var table in otherTables)
        {
            await CreateSchemaObjectInDatabase(table);
        }

        await CreateSchemaObjectInDatabase(initial);

        await Task.Delay(100.Milliseconds());

        var delta = await configured.FindDeltaAsync(theConnection);

        var migration = new SchemaMigration(new ISchemaObjectDelta[] { delta });

        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        await Task.Delay(100.Milliseconds());

        await migration.RollbackAllAsync(theConnection, new SqlServerMigrator());

        var delta2 = await initial.FindDeltaAsync(theConnection);
        delta2.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task columns_forward_and_backwards()
    {
        initial.AddColumn<string>("random");
        configured.AddColumn<string>("other");

        await AssertRollbackIsSuccessful();
    }

    [Fact]
    public async Task indexes_forward_and_backwards()
    {
        initial.ModifyColumn("user_name").AddIndex();
        configured.ModifyColumn("last_name").AddIndex();

        await AssertRollbackIsSuccessful();
    }

    [Fact]
    public async Task changed_index()
    {
        initial.ModifyColumn("user_name").AddIndex();
        configured.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true);

        await AssertRollbackIsSuccessful();
    }

    [Fact]
    public async Task new_fkey_should_be_dropped_on_rollback()
    {
        var states = new Table("rollbacks.states");
        states.AddColumn<int>("id").AsPrimaryKey();

        configured.AddColumn<int>("state_id")
            .ForeignKeyTo(states, "id");

        await AssertRollbackIsSuccessful(states);
    }

    [Fact]
    public async Task if_an_fkey_is_removed_rollback_should_put_it_back()
    {
        var states = new Table("rollbacks.states");
        states.AddColumn<int>("id").AsPrimaryKey();

        initial.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        await AssertRollbackIsSuccessful(states);
    }

    [Fact]
    public async Task changed_primary_key()
    {
        configured.AddColumn<string>("tenant_id");

        await AssertRollbackIsSuccessful();
    }
}
