using System.Diagnostics;
using FirebirdSql.Data.FirebirdClient;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Functions;
using Weasel.Firebird.Procedures;
using Weasel.Firebird.Tables;
using Weasel.Firebird.Triggers;
using Weasel.Firebird.Views;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     Views, routines and triggers are written <c>CREATE OR ALTER</c>, not guarded, and concurrent
///     <c>CREATE OR ALTER</c>s of one object have losers on Firebird 3, 4 and 5: "update conflicts with
///     concurrent update", a unique key violation in the catalog, or -- when the winner commits between
///     the loser's two looks at the catalog -- the object's own "already exists". The statement leaves the
///     same object however often it runs, so the migrator runs a loser again, as it does a guarded
///     statement.
/// </summary>
public class racing_create_or_alter: IntegrationContext
{
    private static Table orders()
    {
        var table = new Table("race_orders");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("note", "VARCHAR(40)");
        return table;
    }

    private static ISchemaObject[] model()
    {
        return
        [
            orders(),
            new View("race_view", "select id, note from race_orders"),
            new View("race_view_of_view", "select id from race_view"),
            new Function("race_function", "CREATE FUNCTION race_function (n INTEGER) RETURNS INTEGER AS BEGIN RETURN n * 2; END"),
            new StoredProcedure("race_procedure", "CREATE PROCEDURE race_procedure RETURNS (n INTEGER) AS BEGIN n = 1; SUSPEND; END"),
            new Trigger("race_trigger", "race_orders", "NEW.note = 'raced'") { Events = TriggerEvents.Insert }
        ];
    }

    private async Task applyOnItsOwnConnectionAsync()
    {
        await using var conn = await OpenConnectionAsync();

        var migrator = new FirebirdMigrator();
        var migration = await SchemaMigration.DetermineAsync(conn, migrator, CancellationToken.None, model());
        await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate);
    }

    /// <summary>
    ///     Ten appliers, released together, twenty-five times over. The table exists before they start, so
    ///     what they race is the <c>CREATE OR ALTER</c>s; fewer, or fewer rounds, and the "already exists"
    ///     loser -- which needs the winner's commit to land inside the loser's statement -- went unseen
    ///     locally and failed on a faster CI runner.
    /// </summary>
    [Fact]
    public async Task racing_appliers_all_succeed_and_the_objects_converge()
    {
        for (var round = 0; round < 25; round++)
        {
            await theConnection.DropSchemaAsync();
            await ApplyAsync(orders());

            using var start = new SemaphoreSlim(0);
            var racers = Enumerable.Range(0, 10).Select(_ => Task.Run(async () =>
            {
                await start.WaitAsync();
                await applyOnItsOwnConnectionAsync();
            })).ToArray();

            start.Release(racers.Length);
            await Task.WhenAll(racers);

            (await DetermineAsync(model())).Difference.ShouldBe(SchemaPatchDifference.None, $"round {round}");
        }
    }

    /// <summary>
    ///     Another kind's "already exists" is a name clash that happens every time, not a race, so it is
    ///     reported on the first attempt: twenty would wait at least 9.5 seconds between them.
    /// </summary>
    private async Task shouldFailOnTheFirstAttemptAsync(ISchemaObject schemaObject, int errorNumber)
    {
        var migrator = new FirebirdMigrator { MaxGuardedStatementAttempts = 20 };
        var migration = await SchemaMigration.DetermineAsync(theConnection, migrator, CancellationToken.None, schemaObject);

        var stopwatch = Stopwatch.StartNew();
        var ex = await Should.ThrowAsync<FbException>(() =>
            migrator.ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate));
        stopwatch.Stop();

        FirebirdMigrator.HasErrorNumber(ex, errorNumber).ShouldBeTrue(ex.Message);
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5), "a name clash recurs, so it is not run again");
    }

    [Fact]
    public async Task a_procedure_named_like_a_table_is_refused_on_the_first_attempt()
    {
        await ApplyAsync(orders());

        await shouldFailOnTheFirstAttemptAsync(
            new StoredProcedure("race_orders", "CREATE PROCEDURE race_orders AS BEGIN END"), 336068740);
    }

    [Fact]
    public async Task a_view_named_like_a_procedure_is_refused_on_the_first_attempt()
    {
        await ApplyAsync(new StoredProcedure("race_named", "CREATE PROCEDURE race_named AS BEGIN END"));

        await shouldFailOnTheFirstAttemptAsync(new View("race_named", "select 1 as x from rdb$database"), 336068743);
    }

    [Theory]
    [InlineData("CREATE OR ALTER VIEW v AS SELECT 1 AS x FROM RDB$DATABASE", true)]
    [InlineData("create  or\n alter procedure p as begin end", true)]
    [InlineData("EXECUTE BLOCK AS BEGIN END", true)]
    [InlineData("-- by hand\n/* and again */ CREATE OR ALTER TRIGGER t FOR x AS BEGIN END", true)]
    [InlineData("CREATE VIEW v AS SELECT 1 AS x FROM RDB$DATABASE", false)]
    [InlineData("ALTER TABLE t ALTER c TYPE BIGINT", false)]
    [InlineData("CREATE OR ALTERNATE", false)]
    public void a_create_or_alter_is_retried_like_a_guarded_statement(string sql, bool retryable)
    {
        FirebirdMigrator.IsRetryable(sql).ShouldBe(retryable);
    }
}
