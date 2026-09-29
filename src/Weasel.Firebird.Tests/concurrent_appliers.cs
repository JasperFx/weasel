using FirebirdSql.Data.FirebirdClient;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     There is no global migration lock on Firebird (as on Oracle, MySQL and SQLite), so replicas
///     starting together race each other. A1 and A2 are what make that safe: every statement is guarded,
///     runs in its own WAIT transaction and is rolled back when it fails, and a guarded statement that
///     loses a catalog race runs again -- a no-op once the winner has created the object.
/// </summary>
public class concurrent_appliers: IntegrationContext
{
    private static ISchemaObject[] model()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();
        states.AddColumn("code", "VARCHAR(2)").AddIndex(x => x.IsUnique = true);

        var people = new Table("people");
        people.AddColumn<int>("id").AsPrimaryKey();
        people.AddColumn<string>("name").AddIndex();
        people.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        var orders = new Table("orders");
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.AddColumn<int>("person_id").ForeignKeyTo(people, "id");
        orders.AddColumn<decimal>("amount").AddIndex();

        return [states, people, orders];
    }

    private async Task applyOnItsOwnConnectionAsync()
    {
        await using var conn = await OpenConnectionAsync();

        var migrator = new FirebirdMigrator();
        var migration = await SchemaMigration.DetermineAsync(conn, migrator, CancellationToken.None, model());
        await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate);
    }

    [Fact]
    public async Task racing_appliers_all_succeed_and_the_schema_converges()
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
            (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$RELATION_CONSTRAINTS WHERE RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'"))
                .ShouldBe(2);
        }
    }

    /// <summary>
    ///     A1: Firebird refuses DDL on a table with uncommitted DML at once under FirebirdClient's
    ///     default NO WAIT. The migrator's WAIT transaction waits for the DML to commit instead.
    /// </summary>
    [Fact]
    public async Task ddl_beside_uncommitted_dml_waits_instead_of_failing()
    {
        var people = new Table("people");
        people.AddColumn<int>("id").AsPrimaryKey();
        people.AddColumn<string>("name");
        await ApplyAsync(people);

        await using var writer = await OpenConnectionAsync();
        await using var transaction = await writer.BeginTransactionAsync();
        await using (var insert = new FbCommand("INSERT INTO people (id, name) VALUES (1, 'x')", writer, transaction))
        {
            await insert.ExecuteNonQueryAsync();
        }

        // The negative control: NO WAIT fails at once.
        await using (var noWait = await OpenConnectionAsync())
        {
            await Should.ThrowAsync<FbException>(async () =>
            {
                await using var tx = await noWait.BeginTransactionAsync(new FbTransactionOptions
                {
                    TransactionBehavior = FbTransactionBehavior.NoWait | FbTransactionBehavior.ReadCommitted
                                                                       | FbTransactionBehavior.RecVersion
                });
                await using var cmd = new FbCommand("CREATE INDEX idx_control ON people (name)", noWait, tx);
                await cmd.ExecuteNonQueryAsync();
                await tx.CommitAsync();
            });
        }

        people.ModifyColumn("name").AddIndex();
        var apply = Task.Run(async () =>
        {
            await using var conn = await OpenConnectionAsync();
            var migrator = new FirebirdMigrator { LockTimeout = TimeSpan.FromSeconds(20) };
            var migration = await SchemaMigration.DetermineAsync(conn, migrator, CancellationToken.None, people);
            await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate);
        });

        await Task.Delay(1000);
        apply.IsCompleted.ShouldBeFalse("the index waits for the uncommitted insert");

        await transaction.CommitAsync();
        await apply;

        (await people.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_lock_that_is_never_released_is_reported_as_a_lock_timeout()
    {
        var people = new Table("people");
        people.AddColumn<int>("id").AsPrimaryKey();
        people.AddColumn<string>("name");
        await ApplyAsync(people);

        await using var writer = await OpenConnectionAsync();
        await using var transaction = await writer.BeginTransactionAsync();
        await using (var insert = new FbCommand("INSERT INTO people (id, name) VALUES (1, 'x')", writer, transaction))
        {
            await insert.ExecuteNonQueryAsync();
        }

        people.ModifyColumn("name").AddIndex();
        var migrator = new FirebirdMigrator { LockTimeout = TimeSpan.FromSeconds(1), MaxGuardedStatementAttempts = 2 };
        var migration = await SchemaMigration.DetermineAsync(theConnection, migrator, CancellationToken.None, people);

        var ex = await Should.ThrowAsync<FbException>(() =>
            migrator.ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate));

        FirebirdMigrator.IsCatalogConflict(ex).ShouldBeTrue(ex.Message);

        await transaction.RollbackAsync();
    }
}
