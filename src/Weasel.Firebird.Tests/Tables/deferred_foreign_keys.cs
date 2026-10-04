using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     A foreign key pointing at a table the same migration has not created yet names something that
///     does not exist, and when two tables reference each other neither can go first. The migration
///     holds such keys back until every table is there -- only those keys, so the DDL for a schema that
///     never had the problem is unchanged.
/// </summary>
public class deferred_foreign_keys: IntegrationContext
{
    [Fact]
    public async Task a_key_to_a_table_created_later_in_the_migration_is_held_back()
    {
        var people = new Table("people");
        people.AddColumn<int>("id").AsPrimaryKey();
        people.AddColumn<int>("state_id").ForeignKeyTo("states", "id");

        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();

        var migration = await DetermineAsync(people, states);
        ((TableDelta)migration.Deltas[0]).HasDeferredForeignKeys.ShouldBeTrue();

        await new FirebirdMigrator().ApplyAllAsync(theConnection, migration, JasperFx.AutoCreate.CreateOrUpdate);

        (await DetermineAsync(people, states)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_key_to_a_table_created_earlier_is_not_held_back()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();

        var people = new Table("people");
        people.AddColumn<int>("id").AsPrimaryKey();
        people.AddColumn<int>("state_id").ForeignKeyTo("states", "id");

        var migration = await DetermineAsync(states, people);

        ((TableDelta)migration.Deltas[1]).HasDeferredForeignKeys.ShouldBeFalse();
    }

    [Fact]
    public async Task an_update_that_adds_a_key_to_a_new_table_holds_it_back()
    {
        var people = new Table("people");
        people.AddColumn<int>("id").AsPrimaryKey();
        people.AddColumn<int>("state_id");
        await CreateSchemaObjectInDatabase(people);

        people.ModifyColumn("state_id").ForeignKeyTo("states", "id");
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();

        var migration = await DetermineAsync(people, states);
        migration.Deltas[0].Difference.ShouldBe(SchemaPatchDifference.Update);
        ((TableDelta)migration.Deltas[0]).HasDeferredForeignKeys.ShouldBeTrue();

        await new FirebirdMigrator().ApplyAllAsync(theConnection, migration, JasperFx.AutoCreate.CreateOrUpdate);

        (await DetermineAsync(people, states)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_table_that_references_itself_needs_nothing_held_back()
    {
        var nodes = new Table("nodes");
        nodes.AddColumn<int>("id").AsPrimaryKey();
        nodes.AddColumn<int>("parent_id").ForeignKeyTo("nodes", "id");

        await ApplyAsync(nodes);

        (await nodes.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_patch_script_writes_held_back_keys_last()
    {
        var left = new Table("lefts");
        left.AddColumn<int>("id").AsPrimaryKey();
        left.AddColumn<int>("right_id").ForeignKeyTo("rights", "id");

        var right = new Table("rights");
        right.AddColumn<int>("id").AsPrimaryKey();
        right.AddColumn<int>("left_id").ForeignKeyTo("lefts", "id");

        var migration = await DetermineAsync(left, right);
        var writer = new StringWriter();
        migration.WriteAllUpdates(writer, new FirebirdMigrator(), JasperFx.AutoCreate.CreateOrUpdate);

        var statements = FirebirdScript.Split(writer.ToString());
        statements.Count.ShouldBe(4);
        statements[0].ShouldContain("CREATE TABLE lefts");
        statements[1].ShouldContain("CREATE TABLE rights");
        statements[2].ShouldContain("fk_rights_left_id", Case.Sensitive, "lefts exists by now, so this key is not held back");
        statements[3].ShouldContain("fk_lefts_right_id", Case.Sensitive, "held back until rights exists");
    }
}
