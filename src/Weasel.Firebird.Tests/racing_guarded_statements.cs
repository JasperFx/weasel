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
///     a racing applier commits in between: Firebird answers with the kind's "already exists" -- "Table
///     @1 already exists" for a guarded <c>CREATE TABLE</c>, "Index @1 already exists" for its index.
///     Run once more, at once, the guard sees the object and the block does nothing.
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

    private async Task raceAsync(ISchemaObject[] objects, int appliers)
    {
        using var start = new SemaphoreSlim(0);
        var racers = Enumerable.Range(0, appliers).Select(_ => Task.Run(async () =>
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
    ///     Appliers released together, round after round: first creating the table, its index and a
    ///     sequence, then adding a column to it. Firebird 3 is where the index's "already exists" showed,
    ///     so it gets sixteen appliers and forty rounds: without the immediate re-run that failed five of
    ///     six local runs, and ten appliers and twenty-five rounds one of four. Firebird 4 and 5 take
    ///     several times as long over the same work, so they get the smaller race.
    /// </summary>
    [Fact]
    public async Task racing_appliers_create_a_table_and_add_a_column_and_all_succeed()
    {
        var (appliers, rounds) = ServerVersion.Major < 4 ? (16, 40) : (10, 25);

        for (var round = 0; round < rounds; round++)
        {
            await theConnection.DropSchemaAsync();

            await raceAsync(model(withAddedColumn: false), appliers);
            (await DetermineAsync(model(withAddedColumn: false))).Difference
                .ShouldBe(SchemaPatchDifference.None, $"round {round}, creating");

            await raceAsync(model(withAddedColumn: true), appliers);
            (await DetermineAsync(model(withAddedColumn: true))).Difference
                .ShouldBe(SchemaPatchDifference.None, $"round {round}, adding a column");
        }
    }

    /// <summary>
    ///     A view holding a table's name gets the same number a lost race does, and the table's guard
    ///     looks for a table, so the block is run once more, at once -- and failing the same way again, it
    ///     surfaces, without waiting out the retries a lock conflict gets.
    /// </summary>
    [Fact]
    public async Task a_view_holding_a_tables_name_surfaces_after_one_immediate_rerun()
    {
        await ExecuteAsync("CREATE VIEW race_taken AS SELECT 1 AS id FROM RDB$DATABASE");

        var table = new Table("race_taken");
        table.AddColumn<int>("id").AsPrimaryKey();

        var waits = new List<TimeSpan>();
        var migrator = new FirebirdMigrator
        {
            MaxGuardedStatementAttempts = 20,
            Wait = (delay, _) =>
            {
                waits.Add(delay);
                return Task.CompletedTask;
            }
        };
        var migration = await SchemaMigration.DetermineAsync(theConnection, migrator, CancellationToken.None, table);

        var stopwatch = Stopwatch.StartNew();
        var ex = await Should.ThrowAsync<FbException>(() =>
            migrator.ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate));
        stopwatch.Stop();

        FirebirdMigrator.HasErrorNumber(ex, 336068740).ShouldBeTrue(ex.Message);
        waits.ShouldBeEmpty("a clash is run again at most once, and at once");
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5), "a clash is not retried like a lock conflict");
        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'RACE_TAKEN' AND RDB$VIEW_BLR IS NOT NULL"))
            .ShouldBe(1);
    }
}
