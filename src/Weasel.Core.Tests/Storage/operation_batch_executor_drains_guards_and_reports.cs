using System.Data.Common;
using Microsoft.Data.Sqlite;
using NSubstitute;
using Shouldly;
using Weasel.Core;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests.StorageHelpers;

using IStorageOperation = Weasel.Storage.IStorageOperation;

/// <summary>
///     weasel#810: the three gaps that kept Marten, Polecat and Fisher on batch loops of their own — the
///     <see cref="ResultSetWalk.SkipNoDataReturned" /> walk's missing drain, the marker an adapter hid and
///     the surplus result set nobody noticed, and the per-command listener. Against a real in-memory
///     SQLite connection, whose reader runs the statements it never reached when it is disposed and
///     swallows their errors, as SQL Server's does. The SQL Server and PostgreSQL runs of the
///     same contract are <c>operation_batch_executor_against_sqlserver</c> and
///     <c>operation_batch_executor_against_postgresql</c>.
/// </summary>
public sealed class operation_batch_executor_drains_guards_and_reports: IAsyncLifetime
{
    private const string Duplicate = "insert into things (id) values (1);";

    private readonly SqliteConnection theConnection = new("Data Source=:memory:");
    private readonly IStorageSession theSession = Substitute.For<IStorageSession>();

    public async ValueTask InitializeAsync()
    {
        await theConnection.OpenAsync();
        await using var command = theConnection.CreateCommand();
        command.CommandText = "create table things (id integer primary key); insert into things (id) values (1);";
        await command.ExecuteNonQueryAsync();
    }

    public async ValueTask DisposeAsync() => await theConnection.DisposeAsync();

    private SqliteCommand compile(IReadOnlyList<IStorageOperation> operations)
    {
        var builder = new Weasel.Sqlite.CommandBuilder();
        OperationBatchExecutor.Configure(builder, operations, theSession);
        var command = builder.Compile();
        command.Connection = theConnection;
        return command;
    }

    private Func<CancellationToken, Task<DbDataReader>> batchOf(IReadOnlyList<IStorageOperation> operations)
    {
        var command = compile(operations);
        return async token => await command.ExecuteReaderAsync(token);
    }

    private static async Task<DbDataReader> execute(DbCommand command, CancellationToken token)
        => await command.ExecuteReaderAsync(token);

    private async Task<long> countThings()
    {
        await using var command = theConnection.CreateCommand();
        command.CommandText = "select count(*) from things";
        return (long)(await command.ExecuteScalarAsync())!;
    }

    // ---- the drain ----

    [Fact]
    public async Task skip_no_data_returned_drains_so_a_statement_behind_a_surplus_result_set_runs_and_fails()
    {
        // The mislabelled "select 2" leaves the walk one result set short of the end. Undrained, the
        // duplicate insert behind it ran when the reader was disposed, which swallowed its error, so the
        // batch "succeeded".
        IStorageOperation[] operations =
        [
            new FakeOperation(OperationRole.Other, sql: "select 1;"),
            new FakeNoDataOperation("select 2;"),
            new FakeNoDataOperation(Duplicate)
        ];

        await Should.ThrowAsync<SqliteException>(() =>
            OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations), ResultSetWalk.SkipNoDataReturned,
                CancellationToken.None));
    }

    [Fact]
    public async Task the_walk_overload_still_drains_a_surplus_result_set_silently()
    {
        // The surplus guard is the options overload's; the ResultSetWalk overload keeps its behaviour,
        // apart from the drain, so it steps over the surplus set to the insert behind it and says nothing.
        IStorageOperation[] operations =
        [
            new FakeOperation(OperationRole.Other, sql: "select 1;"),
            new FakeNoDataOperation("select 2;"),
            new FakeNoDataOperation("insert into things (id) values (2);")
        ];

        await OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations), ResultSetWalk.SkipNoDataReturned,
            CancellationToken.None);

        (await countThings()).ShouldBe(2);
    }

    [Fact]
    public async Task execute_each_with_options_drains_each_operation_s_reader()
    {
        IStorageOperation[] operations = [new FakeOperation(OperationRole.Other, sql: "select 1; " + Duplicate)];

        await Should.ThrowAsync<SqliteException>(() =>
            OperationBatchExecutor.ExecuteEachAsync(operations, operation => compile([operation]), execute,
                OperationBatchOptions.Default,
                CancellationToken.None));
    }

    // ---- the surplus guard ----

    [Fact]
    public async Task a_surplus_result_set_is_refused()
    {
        var first = new FakeOperation(OperationRole.Other, sql: "select 1;");
        IStorageOperation[] operations = [first, new FakeNoDataOperation("select 2;")];

        var thrown = await Should.ThrowAsync<SurplusResultSetsException>(() =>
            OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations), OperationBatchOptions.Default,
                CancellationToken.None));

        thrown.SurplusResultSets.ShouldBe(1);
        first.Read.ShouldBe([1L]);
    }

    [Fact]
    public async Task the_surplus_guard_can_be_turned_off()
    {
        IStorageOperation[] operations =
            [new FakeOperation(OperationRole.Other, sql: "select 1;"), new FakeNoDataOperation("select 2;")];

        await OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations),
            new OperationBatchOptions { SurplusResultSets = SurplusResultSetPolicy.Ignore }, CancellationToken.None);
    }

    [Fact]
    public async Task a_batch_in_which_no_operation_reads_a_result_set_has_nothing_to_misread()
    {
        IStorageOperation[] operations = [new FakeNoDataOperation("select 2;"), new FakeNoDataOperation("select 3;")];

        await OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations), OperationBatchOptions.Default,
            CancellationToken.None);
    }

    [Fact]
    public async Task a_sql_error_in_the_batch_is_reported_ahead_of_the_surplus()
    {
        IStorageOperation[] operations =
        [
            new FakeOperation(OperationRole.Other, sql: "select 1;"),
            new FakeNoDataOperation("select 2;"),
            new FakeNoDataOperation(Duplicate)
        ];

        await Should.ThrowAsync<SqliteException>(() =>
            OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations), OperationBatchOptions.Default,
                CancellationToken.None));
    }

    // ---- the marker through a wrapper ----

    [Fact]
    public async Task a_wrapper_carries_the_marker_of_what_it_wraps_into_the_walk()
    {
        // polecat#831's shape: an adapter around a no-data operation, ahead of one that reads. Hidden, the
        // adapter was handed "select 2"'s set and the last operation read nothing — its conflict signal.
        var marked = new FakeNoDataOperation("insert into things (id) values (3);");
        var last = new FakeOperation(OperationRole.Other, sql: "select 2;");
        IStorageOperation[] operations = [new FakeWrapper(marked), last];

        await OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations), OperationBatchOptions.Default,
            CancellationToken.None);

        last.Read.ShouldBe([2L]);
        marked.Postprocessed.ShouldBeFalse();
    }

    [Fact]
    public void returns_result_set_looks_through_every_wrapper()
    {
        var marked = new FakeNoDataOperation("delete from things;");
        var reads = new FakeOperation(OperationRole.Other, sql: "select 1;");

        OperationBatchExecutor.ReturnsResultSet(marked).ShouldBeFalse();
        OperationBatchExecutor.ReturnsResultSet(new FakeWrapper(new FakeWrapper(marked))).ShouldBeFalse();
        OperationBatchExecutor.ReturnsResultSet(reads).ShouldBeTrue();
        OperationBatchExecutor.ReturnsResultSet(new FakeWrapper(reads)).ShouldBeTrue();
    }

    [Fact]
    public void a_wrapper_offers_an_exception_to_the_transform_of_what_it_wraps()
    {
        var replacement = new InvalidOperationException("mapped");
        var wrapper = new FakeWrapper(new FakeTransformingOperation(Duplicate, replacement));

        OperationBatchExecutor.TryTransform(wrapper, new Exception("raw"), out var transformed).ShouldBeTrue();
        transformed.ShouldBeSameAs(replacement);
    }

    // ---- the listener ----

    [Fact]
    public async Task the_listener_sees_the_command_before_it_runs_and_its_success_after_the_walk()
    {
        var listener = new RecordingListener();
        IStorageOperation[] operations = [new FakeOperation(OperationRole.Other, sql: "select 1;")];
        var command = compile(operations);

        await OperationBatchExecutor.ExecuteAsync(command, operations, execute,
            new OperationBatchOptions { Listener = listener }, CancellationToken.None);

        listener.Events.ShouldBe(["before", "success"]);
        listener.Commands.Single().Command.ShouldBeSameAs(command);
        listener.Commands.Single().Operations.ShouldBe(operations);
    }

    [Fact]
    public async Task a_collected_exception_is_thrown_after_the_command_is_reported_successful()
    {
        var listener = new RecordingListener();
        IStorageOperation[] operations =
        [
            new FakeOperation(OperationRole.Other, sql: "select 1;")
            {
                OnPostprocess = (_, exceptions) =>
                {
                    exceptions.Add(new InvalidOperationException("collected"));
                    return Task.CompletedTask;
                }
            }
        ];

        (await Should.ThrowAsync<InvalidOperationException>(() =>
                OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations),
                    new OperationBatchOptions { Listener = listener }, CancellationToken.None)))
            .Message.ShouldBe("collected");

        listener.Events.ShouldBe(["before", "success"]);
    }

    [Fact]
    public async Task the_listener_logs_the_provider_exception_before_an_operation_transforms_it()
    {
        var listener = new RecordingListener { Replacement = new Exception("never asked") };
        var replacement = new InvalidOperationException("mapped");
        IStorageOperation[] operations = [new FakeTransformingOperation(Duplicate, replacement)];

        (await Should.ThrowAsync<InvalidOperationException>(() =>
                OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations),
                    new OperationBatchOptions { Listener = listener }, CancellationToken.None)))
            .ShouldBeSameAs(replacement);

        // The operation, which knows the most, claimed it, so the store-wide transform was not asked.
        listener.Events.ShouldBe(["before", "failure"]);
        listener.Failures.Single().ShouldBeOfType<SqliteException>();
    }

    [Fact]
    public async Task the_listener_transforms_what_no_operation_claims()
    {
        var listener = new RecordingListener { Replacement = new TimeoutException("store-wide") };
        IStorageOperation[] operations = [new FakeOperation(OperationRole.Insert, sql: Duplicate)];

        (await Should.ThrowAsync<TimeoutException>(() =>
                OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations),
                    new OperationBatchOptions { Listener = listener }, CancellationToken.None)))
            .Message.ShouldBe("store-wide");

        listener.Events.ShouldBe(["before", "failure", "transform"]);
        listener.Failures.Single().ShouldBeOfType<SqliteException>();
    }

    [Fact]
    public async Task an_exception_nobody_transforms_propagates_as_itself()
    {
        var listener = new RecordingListener();
        IStorageOperation[] operations = [new FakeOperation(OperationRole.Insert, sql: Duplicate)];

        var thrown = await Should.ThrowAsync<SqliteException>(() =>
            OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations),
                new OperationBatchOptions { Listener = listener }, CancellationToken.None));

        listener.Failures.Single().ShouldBeSameAs(thrown);
    }

    [Fact]
    public async Task execute_each_reports_every_prepared_command_in_order_and_stops_at_the_failure()
    {
        var listener = new RecordingListener();
        var after = new FakeOperation(OperationRole.Other, sql: "select 1;");
        IStorageOperation[] operations =
        [
            new FakeOperation(OperationRole.Insert, sql: "insert into things (id) values (5);"),
            new FakeOperation(OperationRole.Insert, sql: Duplicate),
            after
        ];

        var prepared = new List<DbCommand>();
        DbCommand prepare(IStorageOperation operation)
        {
            var command = compile([operation]);
            prepared.Add(command);
            return command;
        }

        await Should.ThrowAsync<SqliteException>(() =>
            OperationBatchExecutor.ExecuteEachAsync(operations, prepare, execute,
                new OperationBatchOptions { Listener = listener }, CancellationToken.None));

        listener.Events.ShouldBe(["before", "success", "before", "failure", "transform"]);
        listener.Commands.Select(x => x.Command).ShouldBe(prepared);
        listener.Commands.Select(x => x.Operations.Single()).ShouldBe(operations.Take(2));
        after.Read.ShouldBeEmpty();
    }

    [Fact]
    public async Task execute_each_offers_the_failure_to_the_operation_and_then_the_listener()
    {
        var replacement = new InvalidOperationException("mapped");
        var listener = new RecordingListener();
        IStorageOperation[] operations = [new FakeTransformingOperation(Duplicate, replacement)];

        (await Should.ThrowAsync<InvalidOperationException>(() =>
                OperationBatchExecutor.ExecuteEachAsync(operations,
                    async (operation, token) => await compile([operation]).ExecuteReaderAsync(token),
                    new OperationBatchOptions { Listener = listener }, CancellationToken.None)))
            .ShouldBeSameAs(replacement);

        listener.Events.ShouldBe(["before", "failure"]);
        listener.Commands.Single().Command.ShouldBeNull();
    }
}
