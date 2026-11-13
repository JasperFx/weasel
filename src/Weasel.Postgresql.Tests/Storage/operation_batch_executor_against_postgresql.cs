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

    [Fact]
    public async Task the_listener_sees_the_batch_and_without_a_named_command_every_operation_is_asked_in_order()
    {
        // Unlike SqlClient, Npgsql 10.0.3 leaves DbException.BatchCommand null for an error a later
        // statement raises (measured), so the executor cannot tell which operation's command failed and
        // falls back to asking them all in order, as the ResultSetWalk overload always has. The SQL Server
        // twin of this test shows the failing command's own operation answering.
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
            .ShouldBeSameAs(first);

        listener.Events.ShouldBe(["before", "failure"]);
        listener.Commands.Single().Batch.ShouldBeSameAs(batch);
        var raw = listener.Failures.Single().ShouldBeOfType<PostgresException>();
        raw.SqlState.ShouldBe("P0001");
        raw.BatchCommand.ShouldBeNull();
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
