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

    private const int Appliers = 5;
    private const int Rounds = 10;

    /// <summary>
    ///     <see cref="Appliers" /> appliers race <see cref="Rounds" /> times, each round against objects
    ///     of its own. Every applier determines its migration first and all of them start together, so
    ///     they race the same statements -- the test above races the whole migration, where the first
    ///     lost CREATE TABLE staggers the racers and the statements after it rarely collide. Returns
    ///     every failure an applier reports, with its Firebird error numbers.
    /// </summary>
    private async Task<List<string>> raceAsync(Func<int, ISchemaObject[]> modelFor)
    {
        var failures = new List<string>();

        for (var round = 0; round < Rounds; round++)
        {
            // Each applier gets a model of its own, as each replica would.
            var thisRound = round;
            using var ready = new SemaphoreSlim(0);
            using var start = new SemaphoreSlim(0);

            var racers = Enumerable.Range(0, Appliers).Select(_ => Task.Run(async () =>
            {
                await using var conn = await OpenConnectionAsync();
                var migrator = new FirebirdMigrator();
                var migration = await SchemaMigration.DetermineAsync(conn, migrator, CancellationToken.None,
                    modelFor(thisRound));

                ready.Release();
                await start.WaitAsync();

                try
                {
                    await migrator.ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate);
                    return null;
                }
                catch (Exception e)
                {
                    var firebird = e as FbException ?? e.InnerException as FbException;
                    var numbers = firebird == null ? "" : $" [{string.Join(",", firebird.Errors.Select(x => x.Number))}] {firebird.SQLSTATE}";
                    return $"round {thisRound}: {e.GetType().Name}{numbers}: {e.Message.ReplaceLineEndings(" ")}";
                }
            })).ToArray();

            for (var i = 0; i < Appliers; i++)
            {
                await ready.WaitAsync();
            }

            start.Release(Appliers);
            failures.AddRange((await Task.WhenAll(racers)).OfType<string>());

            (await DetermineAsync(modelFor(round))).Difference.ShouldBe(SchemaPatchDifference.None, $"round {round}");
        }

        return failures;
    }

    private async Task shouldExistOnceAsync(string sql)
        => (await ScalarAsync<int>(sql)).ShouldBe(1, sql);

    /// <summary>
    ///     Two racers that both pass the guard of a CREATE INDEX can both lose: one with the catalog's
    ///     unique key violation (335544665) when it runs, the other with "too many keys defined for
    ///     index" (335544631) when it commits. Both are races, and both are run again.
    /// </summary>
    [Fact]
    public async Task racing_appliers_all_add_the_same_indexes()
    {
        for (var round = 0; round < Rounds; round++)
        {
            await ExecuteAsync(
                $"CREATE TABLE race_ix_{round} (a VARCHAR(120) NOT NULL, b VARCHAR(150) NOT NULL, c VARCHAR(150) NOT NULL, d INTEGER)");
        }

        var failures = await raceAsync(round =>
        {
            var table = new Table($"race_ix_{round}");
            table.AddColumn("a", "VARCHAR(120)").NotNull();
            table.AddColumn("b", "VARCHAR(150)").NotNull();
            table.AddColumn("c", "VARCHAR(150)").NotNull();
            table.AddColumn<int>("d");
            table.Indexes.Add(new IndexDefinition($"ix_race_{round}_abc") { Columns = ["a", "b", "c"] });
            table.Indexes.Add(new IndexDefinition($"ix_race_{round}_cb") { Columns = ["c", "b"] });
            table.Indexes.Add(new IndexDefinition($"ix_race_{round}_d") { Columns = ["d"] });
            return [table];
        });

        failures.ShouldBeEmpty();

        for (var round = 0; round < Rounds; round++)
        {
            foreach (var suffix in new[] { "ABC", "CB", "D" })
            {
                await shouldExistOnceAsync(
                    $"SELECT COUNT(*) FROM RDB$INDICES WHERE RDB$INDEX_NAME = 'IX_RACE_{round}_{suffix}'");
            }
        }
    }

    [Fact]
    public async Task racing_appliers_all_add_the_same_foreign_key()
    {
        for (var round = 0; round < Rounds; round++)
        {
            await ExecuteAsync($"""
                CREATE TABLE race_p_{round} (a VARCHAR(120) NOT NULL, b VARCHAR(150) NOT NULL, CONSTRAINT pk_race_p_{round} PRIMARY KEY (a, b));
                CREATE TABLE race_c_{round} (id INTEGER NOT NULL, a VARCHAR(120) NOT NULL, b VARCHAR(150) NOT NULL, CONSTRAINT pk_race_c_{round} PRIMARY KEY (id));
                """);
        }

        var failures = await raceAsync(round =>
        {
            var parent = new Table($"race_p_{round}");
            parent.AddColumn("a", "VARCHAR(120)").NotNull().AsPrimaryKey();
            parent.AddColumn("b", "VARCHAR(150)").NotNull().AsPrimaryKey();
            parent.PrimaryKeyName = $"pk_race_p_{round}";

            var child = new Table($"race_c_{round}");
            child.AddColumn<int>("id").AsPrimaryKey();
            child.AddColumn("a", "VARCHAR(120)").NotNull();
            child.AddColumn("b", "VARCHAR(150)").NotNull();
            child.PrimaryKeyName = $"pk_race_c_{round}";
            child.ForeignKeys.Add(new ForeignKey($"fk_race_c_{round}")
            {
                LinkedTable = parent.Identifier, ColumnNames = ["a", "b"], LinkedNames = ["a", "b"]
            });

            return [parent, child];
        });

        failures.ShouldBeEmpty();

        for (var round = 0; round < Rounds; round++)
        {
            await shouldExistOnceAsync(
                $"SELECT COUNT(*) FROM RDB$RELATION_CONSTRAINTS WHERE RDB$CONSTRAINT_NAME = 'FK_RACE_C_{round}' AND RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'");
        }
    }

    [Fact]
    public async Task racing_appliers_all_create_the_same_table_column_and_sequence()
    {
        for (var round = 0; round < Rounds; round++)
        {
            await ExecuteAsync($"CREATE TABLE race_t_{round} (id INTEGER NOT NULL)");
        }

        var failures = await raceAsync(round =>
        {
            var existing = new Table($"race_t_{round}");
            existing.AddColumn("id", "INTEGER").NotNull();
            existing.AddColumn<int>("added");

            var created = new Table($"race_n_{round}");
            created.AddColumn<int>("id").AsPrimaryKey();

            return [existing, created, new Sequence($"race_sq_{round}")];
        });

        failures.ShouldBeEmpty();

        for (var round = 0; round < Rounds; round++)
        {
            await shouldExistOnceAsync(
                $"SELECT COUNT(*) FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = 'RACE_T_{round}' AND RDB$FIELD_NAME = 'ADDED'");
            await shouldExistOnceAsync($"SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'RACE_N_{round}'");
            await shouldExistOnceAsync($"SELECT COUNT(*) FROM RDB$GENERATORS WHERE RDB$GENERATOR_NAME = 'RACE_SQ_{round}'");
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

    /// <summary>
    ///     A hand-written script that puts a comment before a guarded block keeps the retry: the first
    ///     attempt times out on the uncommitted insert, and a later one runs once it is rolled back.
    /// </summary>
    [Fact]
    public async Task a_guarded_statement_after_a_comment_is_still_run_again_after_a_lock_timeout()
    {
        var people = new Table("people");
        people.AddColumn<int>("id").AsPrimaryKey();
        people.AddColumn<string>("name");
        await ApplyAsync(people);

        var script = new StringWriter();
        script.WriteLine("SET TERM ^ ;");
        script.WriteLine("-- The name index, guarded so that a second applier skips it");
        script.Write(FirebirdScript.Guarded(
            "SELECT 1 FROM RDB$INDICES WHERE RDB$INDEX_NAME = 'IDX_PEOPLE_NAME'",
            "CREATE INDEX idx_people_name ON people (name)"));
        script.WriteLine("^");
        script.WriteLine("SET TERM ; ^");
        FirebirdScript.Split(script.ToString()).Single().ShouldStartWith("--");

        await using var writer = await OpenConnectionAsync();
        var transaction = await writer.BeginTransactionAsync();
        await using (var insert = new FbCommand("INSERT INTO people (id, name) VALUES (1, 'x')", writer, transaction))
        {
            await insert.ExecuteNonQueryAsync();
        }

        var migrator = new FirebirdMigrator { LockTimeout = TimeSpan.FromSeconds(1), MaxGuardedStatementAttempts = 10 };
        var apply = Task.Run(async () =>
        {
            await using var conn = await OpenConnectionAsync();
            await migrator.ExecuteScriptAsync(conn, script.ToString());
        });

        await Task.Delay(1500);
        await transaction.RollbackAsync();
        await transaction.DisposeAsync();

        await apply;

        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$INDICES WHERE RDB$INDEX_NAME = 'IDX_PEOPLE_NAME'"))
            .ShouldBe(1);
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
