using System.Diagnostics;
using FirebirdSql.Data.FirebirdClient;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     A guarded statement's guard can find nothing and its statement still find the object there, when
///     a racing applier commits in between: Firebird 3 answered a guarded <c>CREATE TABLE</c> with "Table
///     @1 already exists" under ten racing appliers. Run again, the guard sees the table and the block
///     does nothing, so that is a lost race like any other.
/// </summary>
public class racing_guarded_statements: IntegrationContext
{
    private static ISchemaObject[] model(bool withAddedColumn)
    {
        var table = new Table("race_people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("name", "VARCHAR(40)").AddIndex();
        if (withAddedColumn)
        {
            table.AddColumn<int>("age");
        }

        return [table, new Sequence("race_numbers")];
    }

    private async Task raceAsync(ISchemaObject[] objects)
    {
        using var start = new SemaphoreSlim(0);
        var racers = Enumerable.Range(0, 10).Select(_ => Task.Run(async () =>
        {
            await start.WaitAsync();
            await using var conn = await OpenConnectionAsync();

            var migrator = new FirebirdMigrator();
            var migration = await SchemaMigration.DetermineAsync(conn, migrator, CancellationToken.None, objects);
            await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate);
        })).ToArray();

        start.Release(racers.Length);
        await Task.WhenAll(racers);
    }

    /// <summary>
    ///     Ten appliers, released together, twenty-five times: first creating the table, its index and a
    ///     sequence, then adding a column to it. The late "already exists" needs the winner's commit to
    ///     land between the loser's guard and its statement, which is rare locally; the test holds the
    ///     outcome, whatever the losers got.
    /// </summary>
    [Fact]
    public async Task racing_appliers_create_a_table_and_add_a_column_and_all_succeed()
    {
        for (var round = 0; round < 25; round++)
        {
            await theConnection.DropSchemaAsync();

            await raceAsync(model(withAddedColumn: false));
            (await DetermineAsync(model(withAddedColumn: false))).Difference
                .ShouldBe(SchemaPatchDifference.None, $"round {round}, creating");

            await raceAsync(model(withAddedColumn: true));
            (await DetermineAsync(model(withAddedColumn: true))).Difference
                .ShouldBe(SchemaPatchDifference.None, $"round {round}, adding a column");
        }
    }

    /// <summary>
    ///     A view holding a table's name gets the same number a lost race does, and the table's guard
    ///     looks for a table, so the block is run again -- and fails the same way until the attempts run
    ///     out. With three attempts that is two re-runs, after at least 50 and 100 milliseconds.
    /// </summary>
    [Fact]
    public async Task a_view_holding_a_tables_name_surfaces_once_the_attempts_run_out()
    {
        await ExecuteAsync("CREATE VIEW race_taken AS SELECT 1 AS id FROM RDB$DATABASE");

        var table = new Table("race_taken");
        table.AddColumn<int>("id").AsPrimaryKey();

        var migrator = new FirebirdMigrator { MaxGuardedStatementAttempts = 3 };
        var migration = await SchemaMigration.DetermineAsync(theConnection, migrator, CancellationToken.None, table);

        var stopwatch = Stopwatch.StartNew();
        var ex = await Should.ThrowAsync<FbException>(() =>
            migrator.ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate));
        stopwatch.Stop();

        FirebirdMigrator.HasErrorNumber(ex, 336068740).ShouldBeTrue(ex.Message);
        stopwatch.Elapsed.ShouldBeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(150),
            "the number a lost race shares is run again");
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5), "but only as often as the attempts allow");
        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'RACE_TAKEN' AND RDB$VIEW_BLR IS NOT NULL"))
            .ShouldBe(1);
    }
}
