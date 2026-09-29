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
///     Views, routines and triggers are written <c>CREATE OR ALTER</c>, not guarded, and four concurrent
///     <c>CREATE OR ALTER</c>s of one object measured three losers on Firebird 3, 4 and 5 -- "update
///     conflicts with concurrent update", or a unique key violation in the catalog. The statement leaves
///     the same object however often it runs, so the migrator runs a loser again, as it does a guarded
///     statement.
/// </summary>
public class racing_create_or_alter: IntegrationContext
{
    private static ISchemaObject[] model()
    {
        var orders = new Table("race_orders");
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.AddColumn("note", "VARCHAR(40)");

        return
        [
            orders,
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

    [Fact]
    public async Task racing_appliers_all_succeed_and_the_objects_converge()
    {
        for (var round = 0; round < 4; round++)
        {
            await theConnection.DropSchemaAsync();

            using var start = new SemaphoreSlim(0);
            var racers = Enumerable.Range(0, 5).Select(_ => Task.Run(async () =>
            {
                await start.WaitAsync();
                await applyOnItsOwnConnectionAsync();
            })).ToArray();

            start.Release(racers.Length);
            await Task.WhenAll(racers);

            (await DetermineAsync(model())).Difference.ShouldBe(SchemaPatchDifference.None, $"round {round}");
        }
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
