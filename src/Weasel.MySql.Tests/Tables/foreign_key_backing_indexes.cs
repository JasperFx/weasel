using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

/// <summary>
///     MySQL implicitly creates a backing index for every FOREIGN KEY constraint and
///     names it after the constraint. information_schema.STATISTICS reports that index
///     like any other, so a table that declares only a foreign key used to diff as
///     "one extra index" forever, and InnoDB refuses the DROP INDEX that follows
///     (error 1553). See wolverine#3983.
/// </summary>
public class foreign_key_backing_indexes: IntegrationContext
{
    private async Task<Table> createParentAsync(string name)
    {
        await DropTableAsync($"`weasel_testing`.`{name}`");
        var parent = new Table($"weasel_testing.{name}");
        parent.AddColumn<int>("id").AsPrimaryKey();
        await parent.CreateAsync(theConnection);
        return parent;
    }

    [Fact]
    public async Task a_table_declaring_only_a_foreign_key_round_trips_with_no_delta()
    {
        await DropTableAsync("`weasel_testing`.`fkidx_child_1`");
        var parent = await createParentAsync("fkidx_parent_1");

        var expected = new Table("weasel_testing.fkidx_child_1");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<int>("parent_id")
            .ForeignKeyTo(parent.Identifier, "id", onDelete: CascadeAction.Cascade);

        await expected.CreateAsync(theConnection);

        var delta = await expected.FindDeltaAsync(theConnection) as TableDelta;

        delta!.Indexes!.Extras.ShouldBeEmpty();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task applying_changes_twice_against_a_foreign_key_is_a_no_op()
    {
        await DropTableAsync("`weasel_testing`.`fkidx_child_2`");
        var parent = await createParentAsync("fkidx_parent_2");

        var expected = new Table("weasel_testing.fkidx_child_2");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<int>("parent_id")
            .ForeignKeyTo(parent.Identifier, "id", onDelete: CascadeAction.Cascade);

        // First pass creates the table; the second used to emit an unrunnable DROP INDEX.
        await expected.ApplyChangesAsync(theConnection);
        await expected.ApplyChangesAsync(theConnection);

        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task dropping_a_foreign_key_also_clears_its_backing_index()
    {
        await DropTableAsync("`weasel_testing`.`fkidx_child_3`");
        var parent = await createParentAsync("fkidx_parent_3");

        var withKey = new Table("weasel_testing.fkidx_child_3");
        withKey.AddColumn<int>("id").AsPrimaryKey();
        withKey.AddColumn<int>("parent_id").ForeignKeyTo(parent.Identifier, "id");
        await withKey.CreateAsync(theConnection);

        // Same table, minus the constraint. The DROP FOREIGN KEY has to be emitted before
        // the DROP INDEX or InnoDB rejects the batch.
        var withoutKey = new Table("weasel_testing.fkidx_child_3");
        withoutKey.AddColumn<int>("id").AsPrimaryKey();
        withoutKey.AddColumn<int>("parent_id");

        await withoutKey.ApplyChangesAsync(theConnection);

        (await withoutKey.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_index_unrelated_to_a_foreign_key_is_still_reported_as_extra()
    {
        await DropTableAsync("`weasel_testing`.`fkidx_child_4`");
        var parent = await createParentAsync("fkidx_parent_4");

        var actual = new Table("weasel_testing.fkidx_child_4");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<int>("parent_id").ForeignKeyTo(parent.Identifier, "id");
        actual.AddColumn<string>("name").AddIndex();
        await actual.CreateAsync(theConnection);

        var expected = new Table("weasel_testing.fkidx_child_4");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<int>("parent_id").ForeignKeyTo(parent.Identifier, "id");
        expected.AddColumn<string>("name");

        var delta = await expected.FindDeltaAsync(theConnection) as TableDelta;

        // The backing index is protected; the deliberately declared one is not.
        delta!.Indexes!.Extras.Count.ShouldBe(1);
        delta.Indexes.Extras.Single().Columns.ShouldBe(["name"]);

        await expected.ApplyChangesAsync(theConnection);
        (await expected.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_pre_existing_index_that_backs_a_foreign_key_under_another_name_is_protected()
    {
        await DropTableAsync("`weasel_testing`.`fkidx_child_5`");
        var parent = await createParentAsync("fkidx_parent_5");

        // MySQL reuses an existing covering index rather than creating its own, so the
        // backing index here is named idx_..., nothing like the constraint.
        await CreateTableAsync(
            """
            CREATE TABLE `weasel_testing`.`fkidx_child_5` (
                `id` INT NOT NULL,
                `parent_id` INT NULL,
                PRIMARY KEY (`id`),
                INDEX `idx_fkidx_child_5_parent_id` (`parent_id`),
                CONSTRAINT `fk_fkidx_child_5_parent_id` FOREIGN KEY (`parent_id`)
                    REFERENCES `weasel_testing`.`fkidx_parent_5` (`id`)
            ) ENGINE=InnoDB
            """);

        var expected = new Table("weasel_testing.fkidx_child_5");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<int>("parent_id").ForeignKeyTo(parent.Identifier, "id");

        var delta = await expected.FindDeltaAsync(theConnection) as TableDelta;

        delta!.Indexes!.Extras.ShouldBeEmpty();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
