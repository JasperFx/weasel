using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     The migration path -- <c>SchemaMigration.DetermineAsync</c> through the migrator's own command
///     builder -- has to see everything <c>FindDeltaAsync</c> sees. On Oracle it once saw columns only
///     (weasel#474); on Firebird the four queries are four commands, and the reader has to walk all of
///     them for every table, including the ones that do not exist yet.
/// </summary>
public class migration_path_sees_more_than_columns: IntegrationContext
{
    private static Table people()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.AddColumn<int>("state_id");
        return table;
    }

    private async Task<TableDelta> deltaThroughTheMigrationPath(params ISchemaObject[] objects)
        => (TableDelta)(await DetermineAsync(objects)).Deltas.Last();

    [Fact]
    public async Task index_drift_is_seen()
    {
        await CreateSchemaObjectInDatabase(people());

        var model = people();
        model.ModifyColumn("name").AddIndex();

        (await deltaThroughTheMigrationPath(model)).Indexes.Missing.Count.ShouldBe(1);
    }

    [Fact]
    public async Task foreign_key_drift_is_seen()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(states);
        await CreateSchemaObjectInDatabase(people());

        var model = people();
        model.ModifyColumn("state_id").ForeignKeyTo(states, "id");

        (await deltaThroughTheMigrationPath(model)).ForeignKeys.Missing.Count.ShouldBe(1);
    }

    [Fact]
    public async Task primary_key_drift_is_seen()
    {
        await ExecuteAsync("CREATE TABLE people (id INTEGER NOT NULL, name VARCHAR(255), state_id INTEGER)");

        (await deltaThroughTheMigrationPath(people())).PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Create);
    }

    /// <summary>
    ///     A missing table still walks its four result sets, or the next table in the batch reads its
    ///     rows.
    /// </summary>
    [Fact]
    public async Task a_missing_table_between_two_existing_ones_keeps_the_batch_in_step()
    {
        var first = new Table("firsts");
        first.AddColumn<int>("id").AsPrimaryKey();
        first.AddColumn<string>("name").AddIndex();

        var missing = new Table("missing");
        missing.AddColumn<int>("id").AsPrimaryKey();

        var last = people();
        last.ModifyColumn("name").AddIndex();

        await CreateSchemaObjectInDatabase(first);
        await CreateSchemaObjectInDatabase(last);

        var migration = await DetermineAsync(first, missing, last);

        migration.Deltas.Select(x => x.Difference)
            .ShouldBe([SchemaPatchDifference.None, SchemaPatchDifference.Create, SchemaPatchDifference.None]);
    }

    [Fact]
    public async Task many_tables_in_one_batch_are_read_in_step()
    {
        var tables = Enumerable.Range(0, 12).Select(i =>
        {
            var table = new Table($"batched_{i}");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<string>("name").AddIndex();
            return table;
        }).ToArray<ISchemaObject>();

        await ApplyAsync(tables);

        var migration = await DetermineAsync(tables);
        migration.Deltas.ShouldAllBe(x => x.Difference == SchemaPatchDifference.None);
    }
}
