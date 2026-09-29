using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     A rollback is rendered DDL like any other, so it has to be split into statements before Firebird
///     will run it: the default of one batch sends the whole script as a single command, which fails on
///     its second statement (the gap Oracle still has).
/// </summary>
public class rolling_back_a_migration: IntegrationContext
{
    private static Table people()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        return table;
    }

    [Fact]
    public void rollback_sql_is_split_into_one_statement_per_batch()
    {
        var writer = new StringWriter();
        people().WriteDropStatement(new FirebirdMigrator(), writer);

        new FirebirdMigrator().SplitIntoBatches(writer.ToString()).Count.ShouldBe(2);
    }

    [Fact]
    public async Task rolling_back_a_create_drops_the_table()
    {
        var migration = await ApplyAsync(people());

        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await people().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    [Fact]
    public async Task rolling_back_an_update_restores_the_previous_shape()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();

        var original = people();
        original.AddColumn<int>("legacy");
        await ApplyAsync(states, original);

        var changed = people();
        changed.AddColumn<int>("age").AddIndex();
        changed.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        var migration = await ApplyAsync(changed);
        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await original.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task rolling_back_several_tables_runs_every_statement()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();

        var migration = await ApplyAsync(states, people());

        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await states.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await people().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
