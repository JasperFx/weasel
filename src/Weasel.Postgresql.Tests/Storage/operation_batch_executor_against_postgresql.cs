using System.Data.Common;
using Npgsql;
using NSubstitute;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Tests.StorageHelpers;
using Weasel.Storage;
using Xunit;
using IStorageOperation = Weasel.Storage.IStorageOperation;

namespace Weasel.Postgresql.Tests.Storage;

/// <summary>
///     weasel#810 against a real server. Npgsql already surfaces a later statement's error when the reader
///     is disposed, so on PostgreSQL the drain changes where the error is thrown — inside the executor,
///     where the operations' transforms and the listener see it — rather than whether it is. Like
///     <c>SqlBatch</c>, an <c>NpgsqlBatch</c> statement that returns no columns contributes no result set,
///     which is the shape the surplus guard and the wrapper seam exist for. Nothing is created on the
///     server: the failing statement raises from an anonymous block.
/// </summary>
public class operation_batch_executor_against_postgresql
{
    private const string Fail = "do $$ begin raise exception 'weasel810 later statement failed'; end $$";
    private const string NoResultSet = "do $$ begin perform 1; end $$";

    private static async Task<NpgsqlConnection> openAsync()
    {
        var connection = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return connection;
    }

    private static NpgsqlBatch compile(NpgsqlConnection connection, IReadOnlyList<IStorageOperation> operations)
    {
        var builder = new BatchBuilder();
        OperationBatchExecutor.Configure(builder, operations, Substitute.For<IStorageSession>());
        var batch = builder.Compile();
        batch.Connection = connection;
        return batch;
    }

    private static async Task<DbDataReader> execute(DbBatch batch, CancellationToken token)
        => await batch.ExecuteReaderAsync(token);

    [Fact]
    public async Task skip_no_data_returned_surfaces_an_error_behind_a_surplus_result_set()
    {
        await using var connection = await openAsync();
        IStorageOperation[] operations =
        [
            new FakeOperation(OperationRole.Other, sql: "select 1"),
            new FakeNoDataOperation("select 2"),
            new FakeNoDataOperation(Fail)
        ];
        var batch = compile(connection, operations);

        (await Should.ThrowAsync<PostgresException>(() =>
                OperationBatchExecutor.ExecuteAsync(operations, t => execute(batch, t),
                    ResultSetWalk.SkipNoDataReturned, TestContext.Current.CancellationToken)))
            .SqlState.ShouldBe("P0001");
    }

    [Fact]
    public async Task the_error_is_reported_ahead_of_the_surplus_it_hid_behind()
    {
        await using var connection = await openAsync();
        var listener = new RecordingListener();
        IStorageOperation[] operations =
        [
            new FakeOperation(OperationRole.Other, sql: "select 1"),
            new FakeNoDataOperation("select 2"),
            new FakeNoDataOperation(Fail)
        ];

        await Should.ThrowAsync<PostgresException>(() =>
            OperationBatchExecutor.ExecuteAsync(compile(connection, operations), operations, execute,
                new OperationBatchOptions { Listener = listener }, TestContext.Current.CancellationToken));

        // Thrown by the drain, inside the executor, so the listener is told rather than the disposal
        // throwing past it.
        listener.Events.ShouldBe(["before", "failure", "transform"]);
    }

    [Fact]
    public async Task a_surplus_result_set_is_refused()
    {
        await using var connection = await openAsync();
        IStorageOperation[] operations =
            [new FakeOperation(OperationRole.Other, sql: "select 1"), new FakeNoDataOperation("select 2")];

        (await Should.ThrowAsync<SurplusResultSetsException>(() =>
                OperationBatchExecutor.ExecuteAsync(compile(connection, operations), operations, execute,
                    OperationBatchOptions.Default, TestContext.Current.CancellationToken)))
            .SurplusResultSets.ShouldBe(1);
    }

    [Fact]
    public async Task a_wrapped_no_result_operation_takes_no_result_set_from_the_operation_after_it()
    {
        await using var connection = await openAsync();
        var write = new FakeOperation(OperationRole.Update, sql: "select 2");
        IStorageOperation[] operations = [new FakeWrapper(new FakeNoDataOperation(NoResultSet)), write];

        await OperationBatchExecutor.ExecuteAsync(compile(connection, operations), operations, execute,
            OperationBatchOptions.Default, TestContext.Current.CancellationToken);

        write.Read.ShouldBe([2]);
    }

    private const string Ok = "do $$ begin perform 1; end $$";

    private static readonly OperationBatchOptions WithCompletion =
        new() { StatementCompletion = NpgsqlStatementCompletion.HasCompleted };

    [Fact]
    public async Task the_listener_sees_the_batch_and_a_later_statement_s_error_is_translated_by_its_own_operation()
    {
        // weasel#832. Npgsql 10.0.3 leaves DbException.BatchCommand null (measured, and pinned below), so
        // this test used to pin the fallback that asked every operation in order -- and the first
        // operation, whose statement succeeded, claimed the second's error. The error surfaces as the
        // reader advances past the first operation's result set, which executes the second statement
        // and nothing before it, so only the second operation may translate it.
        await using var connection = await openAsync();
        var listener = new RecordingListener();
        var first = new InvalidOperationException("the first operation's");
        var failing = new InvalidOperationException("the failing operation's");
        IStorageOperation[] operations =
        [
            new FakeTransformingOperation("select 1", first),
            new FakeTransformingOperation(Fail, failing)
        ];
        var batch = compile(connection, operations);

        (await Should.ThrowAsync<InvalidOperationException>(() =>
                OperationBatchExecutor.ExecuteAsync(batch, operations, execute,
                    new OperationBatchOptions { Listener = listener }, TestContext.Current.CancellationToken)))
            .ShouldBeSameAs(failing);

        listener.Events.ShouldBe(["before", "failure"]);
        listener.Commands.Single().Batch.ShouldBeSameAs(batch);
        var raw = listener.Failures.Single().ShouldBeOfType<PostgresException>();
        raw.SqlState.ShouldBe("P0001");
        raw.BatchCommand.ShouldBeNull();
    }

    [Fact]
    public async Task when_the_earlier_statement_returns_no_rows_its_completion_rules_its_operation_out()
    {
        // Both statements run when the reader opens, so the reader's position cannot tell them apart.
        // Npgsql records a statement's completion, and PostgreSQL stops at the first error, so the first
        // statement that never completed is the failing one.
        await using var connection = await openAsync();
        var failing = new InvalidOperationException("the failing operation's");
        IStorageOperation[] operations =
        [
            new FakeNoDataTransformingOperation(Ok, new InvalidOperationException("the first operation's")),
            new FakeNoDataTransformingOperation(Fail, failing)
        ];

        (await Should.ThrowAsync<InvalidOperationException>(() =>
                OperationBatchExecutor.ExecuteAsync(compile(connection, operations), operations, execute,
                    WithCompletion, TestContext.Current.CancellationToken)))
            .ShouldBeSameAs(failing);
    }

    [Fact]
    public async Task when_the_earlier_statement_returns_no_rows_and_the_failing_one_does()
    {
        await using var connection = await openAsync();
        var failing = new InvalidOperationException("the failing operation's");
        IStorageOperation[] operations =
        [
            new FakeNoDataTransformingOperation(Ok, new InvalidOperationException("the first operation's")),
            new FakeTransformingOperation("select 1 / 0", failing)
        ];

        (await Should.ThrowAsync<InvalidOperationException>(() =>
                OperationBatchExecutor.ExecuteAsync(compile(connection, operations), operations, execute,
                    WithCompletion, TestContext.Current.CancellationToken)))
            .ShouldBeSameAs(failing);
    }

    [Fact]
    public async Task an_empty_statement_does_not_count_as_the_one_that_never_completed()
    {
        // An empty statement answers EmptyQueryResponse, never CommandComplete, so Npgsql leaves it
        // Unknown even though it ran (measured). Read naively, it is the first statement that never
        // completed.
        await using var connection = await openAsync();
        var failing = new InvalidOperationException("the failing operation's");
        IStorageOperation[] operations =
        [
            new FakeNoDataTransformingOperation(" ", new InvalidOperationException("the empty statement's")),
            new FakeNoDataTransformingOperation(Fail, failing)
        ];

        (await Should.ThrowAsync<InvalidOperationException>(() =>
                OperationBatchExecutor.ExecuteAsync(compile(connection, operations), operations, execute,
                    WithCompletion, TestContext.Current.CancellationToken)))
            .ShouldBeSameAs(failing);
    }

    [Fact]
    public async Task without_statement_completion_a_run_without_result_sets_is_asked_in_statement_order()
    {
        // The documented limit of the position alone: it narrows the error to the statements the reader
        // was executing, here both, and those are asked in order.
        await using var connection = await openAsync();
        var first = new InvalidOperationException("the first operation's");
        IStorageOperation[] operations =
        [
            new FakeNoDataTransformingOperation(Ok, first),
            new FakeNoDataTransformingOperation(Fail, new InvalidOperationException("the failing operation's"))
        ];

        (await Should.ThrowAsync<InvalidOperationException>(() =>
                OperationBatchExecutor.ExecuteAsync(compile(connection, operations), operations, execute,
                    OperationBatchOptions.Default, TestContext.Current.CancellationToken)))
            .ShouldBeSameAs(first);
    }

    [Fact]
    public async Task a_re_executed_batch_does_not_trust_completion_left_over_from_its_last_run()
    {
        // Npgsql never resets StatementType, so on a second execution the first statement already reports
        // complete before it runs. The executor sees that and falls back to statement order rather than
        // read an earlier execution's progress as this one's.
        await using var connection = await openAsync();
        var first = new InvalidOperationException("the first operation's");
        var failing = new InvalidOperationException("the failing operation's");
        IStorageOperation[] operations =
        [
            new FakeNoDataTransformingOperation(Ok, first),
            new FakeNoDataTransformingOperation(Fail, failing)
        ];
        var batch = compile(connection, operations);

        (await Should.ThrowAsync<InvalidOperationException>(() =>
                OperationBatchExecutor.ExecuteAsync(batch, operations, execute, WithCompletion,
                    TestContext.Current.CancellationToken)))
            .ShouldBeSameAs(failing);

        (await Should.ThrowAsync<InvalidOperationException>(() =>
                OperationBatchExecutor.ExecuteAsync(batch, operations, execute, WithCompletion,
                    TestContext.Current.CancellationToken)))
            .ShouldBeSameAs(first);
    }

    [Fact]
    public async Task an_error_its_own_operation_does_not_translate_is_offered_to_no_other()
    {
        await using var connection = await openAsync();
        IStorageOperation[] operations =
        [
            new FakeTransformingOperation("select 1", new InvalidOperationException("the first operation's")),
            new FakeNoDataOperation(Fail)
        ];

        (await Should.ThrowAsync<PostgresException>(() =>
                OperationBatchExecutor.ExecuteAsync(compile(connection, operations), operations, execute,
                    WithCompletion, TestContext.Current.CancellationToken)))
            .SqlState.ShouldBe("P0001");
    }

    [Theory]
    [InlineData("select 1", false, true)]
    [InlineData(Ok, false, true)]
    [InlineData(" ;", null, null)] // an empty statement never reports, and cannot fail
    [InlineData("-- only a comment", null, null)]
    public async Task statement_completion_reads_npgsql_s_statement_type(string sql, bool? beforeRunning,
        bool? afterRunning)
    {
        await using var connection = await openAsync();
        var batch = new NpgsqlBatch(connection) { BatchCommands = { new NpgsqlBatchCommand(sql) } };

        NpgsqlStatementCompletion.HasCompleted(batch.BatchCommands[0]).ShouldBe(beforeRunning);

        await using (var reader = await batch.ExecuteReaderAsync(TestContext.Current.CancellationToken))
        {
            while (await reader.NextResultAsync(TestContext.Current.CancellationToken))
            {
            }
        }

        NpgsqlStatementCompletion.HasCompleted(batch.BatchCommands[0]).ShouldBe(afterRunning);
    }

    [Fact]
    public async Task the_listener_reports_success_once_the_batch_is_drained()
    {
        await using var connection = await openAsync();
        var listener = new RecordingListener();
        var first = new FakeOperation(OperationRole.Other, sql: "select 1");
        var last = new FakeOperation(OperationRole.Other, sql: "select 3");
        IStorageOperation[] operations = [first, new FakeNoDataOperation(NoResultSet), last];

        await OperationBatchExecutor.ExecuteAsync(compile(connection, operations), operations, execute,
            new OperationBatchOptions { Listener = listener }, TestContext.Current.CancellationToken);

        listener.Events.ShouldBe(["before", "success"]);
        first.Read.ShouldBe([1]);
        last.Read.ShouldBe([3]);
    }
}
