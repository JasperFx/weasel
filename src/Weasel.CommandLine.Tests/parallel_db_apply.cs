using System.Collections.Concurrent;
using JasperFx;
using JasperFx.Descriptors;
using Npgsql;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Shouldly;
using Spectre.Console;
using Weasel.Core;
using Weasel.Core.CommandLine;
using Weasel.Core.Migrations;
using Weasel.Postgresql;
using Weasel.Postgresql.Tests;
using Xunit;

namespace Weasel.CommandLine.Tests;

/// <summary>
///     Coverage for weasel#431: db-apply walked its databases strictly sequentially, which at 1,037
///     target databases cost a field deployment 8m41s of dead time for a no-op apply. These pin the
///     shared bounded-parallel batch runner both db-apply and db-assert now use.
/// </summary>
public class DatabaseBatchTests
{
    private static IDatabase fakeDatabase(string databaseName, string schema = "public")
    {
        var descriptor = new DatabaseDescriptor
        {
            Engine = "postgresql", ServerName = "localhost", DatabaseName = databaseName, SchemaOrNamespace = schema
        };

        var database = Substitute.For<IDatabase>();
        database.Describe().Returns(descriptor);

        return database;
    }

    [Fact]
    public async Task runs_every_database_exactly_once()
    {
        var databases = Enumerable.Range(0, 10).Select(i => fakeDatabase($"db{i}")).ToArray();

        var ran = new ConcurrentQueue<IDatabase>();

        var failures = await DatabaseBatch.RunAsync(databases, 4, (database, _) =>
        {
            ran.Enqueue(database);
            return Task.CompletedTask;
        });

        failures.ShouldBeEmpty();
        ran.Count.ShouldBe(10);
        ran.Distinct().Count().ShouldBe(10);
    }

    /// <summary>
    ///     The scheduling contract from the issue thread: parallelize *across* physical databases,
    ///     stay sequential *within* one -- parallel DDL against the same physical database only
    ///     contends on its locks -- and never exceed the requested parallelism overall.
    /// </summary>
    [Fact]
    public async Task bounds_parallelism_and_never_overlaps_databases_sharing_a_physical_database()
    {
        // Eight logical targets over four physical databases, two apiece -- the field topology
        // (1,037 targets at ~2 per physical database) in miniature.
        var databases = Enumerable.Range(0, 8)
            .Select(i => fakeDatabase($"db{i / 2}", schema: $"schema{i}"))
            .ToArray();

        var perDatabaseInFlight = new ConcurrentDictionary<Uri, int>();
        var inFlight = 0;
        var maxInFlight = 0;

        var failures = await DatabaseBatch.RunAsync(databases, 2, async (database, token) =>
        {
            var key = database.Describe().DatabaseUri();

            // No sibling of the same physical database may already be in flight.
            perDatabaseInFlight.AddOrUpdate(key, 1, (_, current) => current + 1).ShouldBe(1);

            var current = Interlocked.Increment(ref inFlight);
            interlockedMax(ref maxInFlight, current);

            await Task.Delay(25, token);

            Interlocked.Decrement(ref inFlight);
            perDatabaseInFlight.AddOrUpdate(key, 0, (_, c) => c - 1);
        });

        failures.ShouldBeEmpty();
        maxInFlight.ShouldBeLessThanOrEqualTo(2);
    }

    private static void interlockedMax(ref int location, int value)
    {
        int current;
        while (value > (current = Volatile.Read(ref location)))
        {
            Interlocked.CompareExchange(ref location, value, current);
        }
    }

    [Fact]
    public async Task a_failure_in_one_database_does_not_stop_the_others()
    {
        var databases = Enumerable.Range(0, 5).Select(i => fakeDatabase($"db{i}")).ToArray();
        var poisoned = databases[2];
        var boom = new InvalidOperationException("boom");

        var ran = new ConcurrentQueue<IDatabase>();

        var failures = await DatabaseBatch.RunAsync(databases, 3, (database, _) =>
        {
            ran.Enqueue(database);
            return ReferenceEquals(database, poisoned) ? Task.FromException(boom) : Task.CompletedTask;
        });

        // Every database was still attempted...
        ran.Count.ShouldBe(5);

        // ...and the one failure came back attributed to its database, exception intact.
        var failure = failures.ShouldHaveSingleItem();
        failure.Database.ShouldBeSameAs(poisoned);
        failure.Exception.ShouldBeSameAs(boom);
    }

    [Fact]
    public async Task collects_every_failure_not_just_the_first()
    {
        var databases = Enumerable.Range(0, 6).Select(i => fakeDatabase($"db{i}")).ToArray();

        var failures = await DatabaseBatch.RunAsync(databases, 2, (database, _) =>
        {
            var name = database.Describe().DatabaseName;
            return name is "db1" or "db4"
                ? Task.FromException(new InvalidOperationException(name))
                : Task.CompletedTask;
        });

        failures.Count.ShouldBe(2);
        failures.Select(x => x.Exception.Message).OrderBy(x => x).ShouldBe(new[] { "db1", "db4" });
    }

    [Fact]
    public async Task cancellation_stops_the_batch_and_propagates()
    {
        var databases = Enumerable.Range(0, 5).Select(i => fakeDatabase($"db{i}")).ToArray();

        using var cts = new CancellationTokenSource();
        var ran = 0;

        var exception = await Record.ExceptionAsync(() =>
            DatabaseBatch.RunAsync(databases, 1, (_, _) =>
            {
                Interlocked.Increment(ref ran);
                cts.Cancel();
                return Task.CompletedTask;
            }, cts.Token));

        exception.ShouldBeAssignableTo<OperationCanceledException>();

        // The batch stopped -- it did not keep grinding through the remaining databases.
        ran.ShouldBeLessThan(5);
    }

    /// <summary>
    ///     Sequential is just parallelism 1, and nonsense values fall back to it rather than throwing --
    ///     the flag defaults to 1 precisely so that nothing changes for anyone who does not opt in.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(0)]
    [InlineData(-5)]
    public async Task parallelism_of_one_or_less_runs_strictly_sequentially(int maxParallel)
    {
        var databases = Enumerable.Range(0, 4).Select(i => fakeDatabase($"db{i}")).ToArray();

        var inFlight = 0;
        var ran = 0;

        var failures = await DatabaseBatch.RunAsync(databases, maxParallel, async (_, _) =>
        {
            Interlocked.Increment(ref inFlight).ShouldBe(1);
            Interlocked.Increment(ref ran);
            await Task.Delay(10);
            Interlocked.Decrement(ref inFlight);
        });

        failures.ShouldBeEmpty();
        ran.ShouldBe(4);
    }
}

[Collection("integration")]
public class parallel_db_apply: IntegrationContext
{
    /// <summary>
    ///     A second physical database on the same server (the stock `postgres` maintenance database), so
    ///     the physical-database grouping actually produces more than one group to parallelize across.
    /// </summary>
    private static string alternateConnectionString()
    {
        return new NpgsqlConnectionStringBuilder(ConnectionSource.ConnectionString) { Database = "postgres" }
            .ConnectionString;
    }

    private static async Task dropAlternateSchema(string schemaName)
    {
        await using var conn = new NpgsqlConnection(alternateConnectionString());
        await conn.DropSchemaAsync(schemaName);
    }

    private static IDatabase poisonedDatabase(string databaseName)
    {
        var poisoned = Substitute.For<IDatabase>();
        poisoned.Describe().Returns(new DatabaseDescriptor
        {
            Engine = "postgresql", ServerName = "localhost", DatabaseName = databaseName
        });
        poisoned.Migrator.Returns(new PostgresqlMigrator());
        poisoned.ApplyAllConfiguredChangesToDatabaseAsync(
                Arg.Any<AutoCreate?>(), Arg.Any<ReconnectionOptions?>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("boom"));

        return poisoned;
    }

    [Fact]
    public async Task parallel_apply_touches_every_database_across_physical_databases()
    {
        await DropSchema("par_one");
        await DropSchema("par_two");
        await DropSchema("par_three");
        await dropAlternateSchema("par_alt");

        Databases["one"].Features["one"].AddTable("par_one", "names");
        Databases["two"].Features["two"].AddTable("par_two", "names");
        Databases["three"].Features["three"].AddTable("par_three", "names");

        var alternate = new TestDatabaseWithTables(AutoCreate.CreateOrUpdate, "alternate", alternateConnectionString());
        alternate.Features["alt"].AddTable("par_alt", "names");

        var success = await ExecuteCommand<ApplyCommand>(input => input.ParallelFlag = 4, alternate);
        success.ShouldBeTrue();

        await AssertAllDatabasesMatchConfiguration();
        await alternate.AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task a_failing_database_does_not_stop_the_rest_and_surfaces_an_aggregate()
    {
        await DropSchema("agg_one");
        await DropSchema("agg_two");

        Databases["one"].Features["one"].AddTable("agg_one", "names");
        Databases["two"].Features["two"].AddTable("agg_two", "names");

        var aggregate = await Should.ThrowAsync<AggregateException>(() =>
            ExecuteCommand<ApplyCommand>(input => input.ParallelFlag = 4, poisonedDatabase("does_not_exist")));

        // The one failure came through wrapped with its database identity, original exception intact
        // -- and letting the AggregateException escape Execute is what turns the exit code non-zero.
        var failure = aggregate.InnerExceptions.ShouldHaveSingleItem().ShouldBeOfType<DatabaseApplyException>();
        failure.Message.ShouldContain("does_not_exist");
        failure.InnerException!.Message.ShouldBe("boom");

        // The healthy databases were still fully applied.
        await AssertAllDatabasesMatchConfiguration();
    }

    /// <summary>
    ///     Exactly the same failure policy at --parallel 1: always finish the rest, always aggregate.
    ///     Making the policy a function of the parallelism value ("fail fast at 1, aggregate at 8") is
    ///     the kind of thing that reads as a bug later, per the issue thread.
    /// </summary>
    [Fact]
    public async Task sequential_apply_also_finishes_the_rest_and_aggregates_failures()
    {
        await DropSchema("seq_agg");

        Databases["one"].Features["one"].AddTable("seq_agg", "names");

        var aggregate = await Should.ThrowAsync<AggregateException>(() =>
            ExecuteCommand<ApplyCommand>(null, poisonedDatabase("poisoned_sequential")));

        aggregate.InnerExceptions.ShouldHaveSingleItem().ShouldBeOfType<DatabaseApplyException>()
            .Message.ShouldContain("poisoned_sequential");

        await AssertAllDatabasesMatchConfiguration();
    }

    [Fact]
    public async Task per_database_ddl_is_flushed_as_an_attributable_unit_with_completion_counter_and_summary()
    {
        await DropSchema("attribution_one");
        await DropSchema("attribution_two");

        Databases["one"].Features["one"].AddTable("attribution_one", "alpha");
        Databases["two"].Features["two"].AddTable("attribution_two", "beta");

        var text = await captureConsole(() => ExecuteCommand<ApplyCommand>(input => input.ParallelFlag = 2));

        // The completion counter counts completions -- under parallelism a positional (i+1)/total
        // stops meaning anything.
        text.ShouldContain("(1/2)");
        text.ShouldContain("(2/2)");

        // The rollup block.
        text.ShouldContain("Applied 2 database(s): 0 unchanged, 2 migrated, 0 failed.");

        // Each database's DDL was flushed as one unit directly above its own completion line: the text
        // before each "Successfully applied" line mentions exactly one of the two schemas.
        var lines = text.Split('\n');
        var statusIndexes = lines.Select((line, index) => (line, index))
            .Where(x => x.line.Contains("Successfully applied migrations"))
            .Select(x => x.index)
            .ToArray();
        statusIndexes.Length.ShouldBe(2);

        var firstSegment = string.Join('\n', lines[..statusIndexes[0]]);
        var secondSegment = string.Join('\n', lines[(statusIndexes[0] + 1)..statusIndexes[1]]);

        foreach (var segment in new[] { firstSegment, secondSegment })
        {
            var mentionsOne = segment.Contains("attribution_one");
            var mentionsTwo = segment.Contains("attribution_two");

            mentionsOne.ShouldNotBe(mentionsTwo, customMessage:
                $"Expected exactly one database's DDL per segment, but got one:{mentionsOne} two:{mentionsTwo} in segment:\n{segment}");
        }

        (firstSegment + secondSegment).ShouldContain("attribution_one");
        (firstSegment + secondSegment).ShouldContain("attribution_two");
    }

    [Fact]
    public async Task no_op_reapply_reports_every_database_unchanged()
    {
        await DropSchema("noop_one");
        await DropSchema("noop_two");

        Databases["one"].Features["one"].AddTable("noop_one", "names");
        Databases["two"].Features["two"].AddTable("noop_two", "names");

        (await ExecuteCommand<ApplyCommand>()).ShouldBeTrue();

        // The erdtsieck deploy in miniature: everything already applied, run it again in parallel.
        var text = await captureConsole(async () =>
            (await ExecuteCommand<ApplyCommand>(input => input.ParallelFlag = 2)).ShouldBeTrue());

        text.ShouldContain("Applied 2 database(s): 2 unchanged, 0 migrated, 0 failed.");
    }

    /// <summary>
    ///     Everything ApplyCommand writes goes through the static <see cref="AnsiConsole" />, so the
    ///     capture swaps the global console for the duration. Safe here because the "integration"
    ///     collection runs serially.
    /// </summary>
    internal static async Task<string> captureConsole(Func<Task> action)
    {
        var output = new StringWriter();
        var testConsole = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Out = new AnsiConsoleOutput(output)
        });
        testConsole.Profile.Width = 500;

        var original = AnsiConsole.Console;
        AnsiConsole.Console = testConsole;

        try
        {
            await action();
        }
        finally
        {
            AnsiConsole.Console = original;
        }

        return output.ToString();
    }
}

[Collection("integration")]
public class parallel_db_assert: IntegrationContext
{
    [Fact]
    public async Task assert_honors_the_parallel_flag_and_still_checks_every_database()
    {
        await DropSchema("passert_one");
        await DropSchema("passert_two");

        Databases["one"].Features["one"].AddTable("passert_one", "names");
        Databases["two"].Features["two"].AddTable("passert_two", "names");

        (await ExecuteCommand<ApplyCommand>()).ShouldBeTrue();

        var success = await ExecuteCommand<AssertCommand>(input => input.ParallelFlag = 2);
        success.ShouldBeTrue();
    }

    [Fact]
    public async Task a_failed_assertion_fails_the_command_but_every_database_is_still_checked()
    {
        await DropSchema("passert_three");
        await DropSchema("passert_four");

        Databases["one"].Features["one"].AddTable("passert_three", "names");
        Databases["two"].Features["two"].AddTable("passert_four", "names");

        // Only apply database one; database two's schema is missing and must fail the assertion.
        await Databases["one"].ApplyAllConfiguredChangesToDatabaseAsync();

        var text = await parallel_db_apply.captureConsole(async () =>
        {
            var success = await ExecuteCommand<AssertCommand>(input => input.ParallelFlag = 2);
            success.ShouldBeFalse();
        });

        // The failure did not stop the batch: the healthy database was still asserted and reported.
        text.ShouldContain("No database differences detected for 'one'.");
        text.ShouldContain("does not match the configuration!");
    }
}
