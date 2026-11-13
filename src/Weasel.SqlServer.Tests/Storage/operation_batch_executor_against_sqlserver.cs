using System.Data.Common;
using Microsoft.Data.SqlClient;
using NSubstitute;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Tests.StorageHelpers;
using Weasel.Storage;
using Xunit;
using IStorageOperation = Weasel.Storage.IStorageOperation;

namespace Weasel.SqlServer.Tests.Storage;

/// <summary>
///     weasel#810 against a real server. SQL Server is where the missing drain bit: an <c>SqlBatch</c>
///     surfaces a per-command error only when the reader advances into that command, so an error behind
///     a result set the walk never advanced past was swallowed by the reader's disposal (polecat#273 E2b).
///     And it is where polecat#831 was found: a command that is pure DML contributes no result set at
///     all. Nothing is created on the server — the failing statement is a <c>THROW</c>.
/// </summary>
public class operation_batch_executor_against_sqlserver
{
    private const string Fail = "THROW 51810, 'weasel810 later statement failed', 1";

    private static async Task<SqlConnection> openAsync()
    {
        var connection = new SqlConnection(ConnectionSource.ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return connection;
    }

    private static SqlBatch compile(SqlConnection connection, IReadOnlyList<IStorageOperation> operations)
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
    public async Task skip_no_data_returned_drains_so_an_error_behind_a_surplus_result_set_surfaces()
    {
        // Through the original ResultSetWalk overload: the drain is the bug fix, so it is not opt-in.
        await using var connection = await openAsync();
        IStorageOperation[] operations =
        [
            new FakeOperation(OperationRole.Other, sql: "select 1"),
            new FakeNoDataOperation("select 2"),
            new FakeNoDataOperation(Fail)
        ];
        var batch = compile(connection, operations);

        var thrown = await Should.ThrowAsync<SqlException>(() =>
            OperationBatchExecutor.ExecuteAsync(operations, t => execute(batch, t), ResultSetWalk.SkipNoDataReturned,
                TestContext.Current.CancellationToken));

        thrown.Number.ShouldBe(51810);
    }

    [Fact]
    public async Task the_error_is_reported_ahead_of_the_surplus_it_hid_behind()
    {
        await using var connection = await openAsync();
        IStorageOperation[] operations =
        [
            new FakeOperation(OperationRole.Other, sql: "select 1"),
            new FakeNoDataOperation("select 2"),
            new FakeNoDataOperation(Fail)
        ];

        await Should.ThrowAsync<SqlException>(() =>
            OperationBatchExecutor.ExecuteAsync(compile(connection, operations), operations, execute,
                OperationBatchOptions.Default, TestContext.Current.CancellationToken));
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
    public async Task a_wrapped_dml_operation_takes_no_result_set_from_the_operation_after_it()
    {
        // polecat#831: [DML, versioned write]. With the marker hidden behind the adapter, the DML's turn
        // stepped past the write's own result set and the write read nothing — a false conflict.
        await using var connection = await openAsync();
        var write = new FakeOperation(OperationRole.Update, sql: "select 2");
        IStorageOperation[] operations = [new FakeWrapper(new FakeNoDataOperation("declare @x int = 1")), write];

        await OperationBatchExecutor.ExecuteAsync(compile(connection, operations), operations, execute,
            OperationBatchOptions.Default, TestContext.Current.CancellationToken);

        write.Read.ShouldBe([2]);
    }

    [Fact]
    public async Task the_listener_sees_the_batch_and_the_failing_command_s_own_operation_transforms()
    {
        // polecat#781: both operations would claim the error; only the one whose command failed may.
        await using var connection = await openAsync();
        var listener = new RecordingListener();
        var notMine = new InvalidOperationException("the first operation's");
        var mine = new InvalidOperationException("the failing operation's");
        IStorageOperation[] operations =
        [
            new FakeTransformingOperation("select 1", notMine),
            new FakeTransformingOperation(Fail, mine)
        ];
        var batch = compile(connection, operations);

        (await Should.ThrowAsync<InvalidOperationException>(() =>
                OperationBatchExecutor.ExecuteAsync(batch, operations, execute,
                    new OperationBatchOptions { Listener = listener }, TestContext.Current.CancellationToken)))
            .ShouldBeSameAs(mine);

        listener.Events.ShouldBe(["before", "failure"]);
        listener.Commands.Single().Batch.ShouldBeSameAs(batch);
        listener.Failures.Single().ShouldBeOfType<SqlException>().Number.ShouldBe(51810);
    }

    [Fact]
    public async Task the_listener_reports_success_once_the_batch_is_drained()
    {
        await using var connection = await openAsync();
        var listener = new RecordingListener();
        var first = new FakeOperation(OperationRole.Other, sql: "select 1");
        var last = new FakeOperation(OperationRole.Other, sql: "select 3");
        IStorageOperation[] operations = [first, new FakeNoDataOperation("declare @x int = 2"), last];

        await OperationBatchExecutor.ExecuteAsync(compile(connection, operations), operations, execute,
            new OperationBatchOptions { Listener = listener }, TestContext.Current.CancellationToken);

        listener.Events.ShouldBe(["before", "success"]);
        first.Read.ShouldBe([1]);
        last.Read.ShouldBe([3]);
    }
}
