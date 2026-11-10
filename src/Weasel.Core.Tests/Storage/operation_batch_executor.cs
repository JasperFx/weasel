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
///     Against a real in-memory SQLite connection, whose readers really do expose several result sets
///     for a multi-statement command — and expose none for a statement that returns no columns, which is
///     exactly the shape <see cref="ResultSetWalk.SkipNoDataReturned" /> exists for.
/// </summary>
public sealed class operation_batch_executor: IAsyncLifetime
{
    private readonly SqliteConnection theConnection = new("Data Source=:memory:");
    private readonly IStorageSession theSession = Substitute.For<IStorageSession>();

    public async ValueTask InitializeAsync()
    {
        await theConnection.OpenAsync();
        await using var command = theConnection.CreateCommand();
        command.CommandText = "create table things (id integer primary key)";
        await command.ExecuteNonQueryAsync();
    }

    public async ValueTask DisposeAsync() => await theConnection.DisposeAsync();

    private Func<CancellationToken, Task<DbDataReader>> batchOf(IReadOnlyList<IStorageOperation> operations)
    {
        var builder = new Weasel.Sqlite.CommandBuilder();
        OperationBatchExecutor.Configure(builder, operations, theSession);
        var command = builder.Compile();
        command.Connection = theConnection;
        return async token => await command.ExecuteReaderAsync(token);
    }

    private Func<IStorageOperation, CancellationToken, Task<DbDataReader>> eachOne()
        => async (operation, token) =>
        {
            var builder = new Weasel.Sqlite.CommandBuilder();
            operation.ConfigureCommand(builder, theSession);
            var command = builder.Compile();
            command.Connection = theConnection;
            return await command.ExecuteReaderAsync(token);
        };

    [Fact]
    public async Task advance_after_each_gives_every_operation_its_own_result_set()
    {
        var operations = new[]
        {
            new FakeOperation(OperationRole.Other, sql: "select 1;"),
            new FakeOperation(OperationRole.Other, sql: "select 2;"),
            new FakeOperation(OperationRole.Other, sql: "select 3;")
        };

        await OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations), ResultSetWalk.AdvanceAfterEach,
            CancellationToken.None);

        operations.Select(x => x.Read.Single()).ShouldBe([1L, 2L, 3L]);
    }

    [Fact]
    public async Task skip_no_data_returned_does_not_advance_past_a_marked_operation()
    {
        // The insert surfaces no result set on SQLite, so advancing past it would hand "select 2" to
        // nobody and leave the last operation reading an exhausted reader.
        var first = new FakeOperation(OperationRole.Other, sql: "select 1;");
        var marked = new FakeNoDataOperation("insert into things (id) values (10);");
        var last = new FakeOperation(OperationRole.Other, sql: "select 2;");
        IStorageOperation[] operations = [first, marked, last];

        await OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations), ResultSetWalk.SkipNoDataReturned,
            CancellationToken.None);

        first.Read.ShouldBe([1L]);
        last.Read.ShouldBe([2L]);
        marked.Postprocessed.ShouldBeFalse();
    }

    [Fact]
    public async Task a_provider_exception_is_offered_to_the_operations_transforms()
    {
        var replacement = new InvalidOperationException("mapped");
        IStorageOperation[] operations =
        [
            new FakeOperation(OperationRole.Other, sql: "insert into things (id) values (1);"),
            new FakeTransformingOperation("insert into things (id) values (1);", replacement)
        ];

        var thrown = await Should.ThrowAsync<InvalidOperationException>(() =>
            OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations), ResultSetWalk.AdvanceAfterEach,
                CancellationToken.None));

        thrown.ShouldBeSameAs(replacement);
    }

    [Fact]
    public async Task an_untransformed_provider_exception_propagates_as_itself()
    {
        IStorageOperation[] operations =
        [
            new FakeOperation(OperationRole.Other, sql: "insert into things (id) values (1);"),
            new FakeOperation(OperationRole.Other, sql: "insert into things (id) values (1);")
        ];

        await Should.ThrowAsync<SqliteException>(() =>
            OperationBatchExecutor.ExecuteAsync(operations, batchOf(operations), ResultSetWalk.AdvanceAfterEach,
                CancellationToken.None));
    }

    [Fact]
    public async Task one_collected_exception_is_thrown_as_itself_and_several_as_an_aggregate()
    {
        FakeOperation collecting(string message) => new(OperationRole.Other, sql: "select 1;")
        {
            OnPostprocess = (_, exceptions) =>
            {
                exceptions.Add(new InvalidOperationException(message));
                return Task.CompletedTask;
            }
        };

        IStorageOperation[] one = [collecting("only")];
        (await Should.ThrowAsync<InvalidOperationException>(() =>
                OperationBatchExecutor.ExecuteAsync(one, batchOf(one), ResultSetWalk.AdvanceAfterEach,
                    CancellationToken.None)))
            .Message.ShouldBe("only");

        IStorageOperation[] two = [collecting("a"), collecting("b")];
        (await Should.ThrowAsync<AggregateException>(() =>
                OperationBatchExecutor.ExecuteAsync(two, batchOf(two), ResultSetWalk.AdvanceAfterEach,
                    CancellationToken.None)))
            .InnerExceptions.Select(x => x.Message).ShouldBe(["a", "b"]);
    }

    [Fact]
    public async Task execute_each_runs_every_operation_on_its_own_reader_in_order()
    {
        var operations = new[]
        {
            new FakeOperation(OperationRole.Insert, sql: "insert into things (id) values (5)"),
            new FakeOperation(OperationRole.Other, sql: "select count(*) from things")
        };

        await OperationBatchExecutor.ExecuteEachAsync(operations, eachOne(), CancellationToken.None);

        operations[1].Read.ShouldBe([1L]);
    }

    [Fact]
    public async Task execute_each_offers_a_failure_to_that_operation_s_transform_and_stops()
    {
        var replacement = new InvalidOperationException("mapped");
        var after = new FakeOperation(OperationRole.Other, sql: "select 1");
        IStorageOperation[] operations =
        [
            new FakeOperation(OperationRole.Insert, sql: "insert into things (id) values (1)"),
            new FakeTransformingOperation("insert into things (id) values (1)", replacement),
            after
        ];

        (await Should.ThrowAsync<InvalidOperationException>(() =>
                OperationBatchExecutor.ExecuteEachAsync(operations, eachOne(), CancellationToken.None)))
            .ShouldBeSameAs(replacement);

        after.Read.ShouldBeEmpty();
    }

    [Fact]
    public void configure_gives_each_unterminated_operation_its_own_batch_command()
    {
        // QueuedSqlOperation and WhereStorageOperation end without a ';', so they are only safe on a
        // builder whose StartNewCommand really splits. Both DbBatch builders do; compiling needs no server.
        IStorageOperation[] operations =
        [
            new QueuedSqlOperation("insert into things (id) values (?);", '?', 1),
            new WhereStorageOperation(new DeleteHead(), typeof(string), new Literal("id = 1"), "t", null,
                OperationRole.Deletion),
            new QueuedSqlOperation("select 1", '?')
        ];

        var postgres = new Weasel.Postgresql.BatchBuilder();
        OperationBatchExecutor.Configure(postgres, operations, theSession);
        var npgsqlBatch = postgres.Compile();
        npgsqlBatch.BatchCommands.Count.ShouldBe(3);
        npgsqlBatch.BatchCommands[1].CommandText.ShouldStartWith("delete from things where tenant_id = ");

        var sqlServer = new Weasel.SqlServer.BatchBuilder();
        OperationBatchExecutor.Configure(sqlServer, operations, theSession);
        sqlServer.Compile().BatchCommands.Count.ShouldBe(3);
    }

    private sealed class DeleteHead: IOperationFragment
    {
        public void Apply(ICommandBuilder builder) => builder.Append("delete from things");
        public OperationRole Role() => OperationRole.Deletion;
    }

    private sealed class Literal(string sql): Weasel.Core.SqlGeneration.ISqlFragment
    {
        public void Apply(ICommandBuilder builder) => builder.Append(sql);
    }

    [Fact]
    public async Task an_empty_batch_never_opens_a_reader()
    {
        var opened = false;

        await OperationBatchExecutor.ExecuteAsync([], _ =>
        {
            opened = true;
            throw new InvalidOperationException();
        }, ResultSetWalk.AdvanceAfterEach, CancellationToken.None);

        opened.ShouldBeFalse();
    }

    [Fact]
    public void throw_if_any_keeps_the_original_stack_trace()
    {
        Exception captured;
        try
        {
            throw new InvalidOperationException("original");
        }
        catch (Exception e)
        {
            captured = e;
        }

        var thrown = Should.Throw<InvalidOperationException>(() => OperationBatchExecutor.ThrowIfAny([captured]));
        thrown.StackTrace!.ShouldContain(nameof(throw_if_any_keeps_the_original_stack_trace));
    }
}
