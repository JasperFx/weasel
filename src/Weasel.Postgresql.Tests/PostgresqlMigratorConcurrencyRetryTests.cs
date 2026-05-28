using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Shouldly;
using Weasel.Postgresql;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     Unit coverage for the transient-catalog-concurrency predicate that drives
///     the bounded retry in <see cref="PostgresqlMigrator" />.executeDelta
///     (weasel#293, follow-up to #282). The retry itself races on the catalog and
///     can't be exercised deterministically, but the classification of which
///     SQLSTATEs are retry-safe is a pure function and is fully testable here.
/// </summary>
public class PostgresqlMigratorConcurrencyRetryTests
{
    [Fact]
    public void serialization_failure_is_transient()
    {
        PostgresqlMigrator.IsTransientCatalogConcurrency(PostgresErrorCodes.SerializationFailure, "could not serialize access")
            .ShouldBeTrue();
    }

    [Fact]
    public void deadlock_detected_is_transient()
    {
        PostgresqlMigrator.IsTransientCatalogConcurrency(PostgresErrorCodes.DeadlockDetected, "deadlock detected")
            .ShouldBeTrue();
    }

    [Fact]
    public void internal_error_with_tuple_concurrently_updated_is_transient()
    {
        PostgresqlMigrator.IsTransientCatalogConcurrency(PostgresErrorCodes.InternalError, "tuple concurrently updated")
            .ShouldBeTrue();
    }

    [Fact]
    public void internal_error_matches_message_case_insensitively()
    {
        PostgresqlMigrator.IsTransientCatalogConcurrency(PostgresErrorCodes.InternalError, "Tuple Concurrently Updated")
            .ShouldBeTrue();
    }

    [Fact]
    public void internal_error_can_appear_inside_a_larger_message()
    {
        PostgresqlMigrator
            .IsTransientCatalogConcurrency(PostgresErrorCodes.InternalError,
                "XX000: tuple concurrently updated while creating function")
            .ShouldBeTrue();
    }

    [Fact]
    public void internal_error_with_an_unrelated_message_is_not_transient()
    {
        // XX000 is a catch-all internal_error — we must not blanket-retry it.
        PostgresqlMigrator.IsTransientCatalogConcurrency(PostgresErrorCodes.InternalError, "some other internal error")
            .ShouldBeFalse();
    }

    [Fact]
    public void internal_error_with_null_message_is_not_transient()
    {
        PostgresqlMigrator.IsTransientCatalogConcurrency(PostgresErrorCodes.InternalError, null)
            .ShouldBeFalse();
    }

    [Theory]
    [InlineData("42P06")] // duplicate_schema — handled by the #282 DO-block, not here
    [InlineData("23505")] // unique_violation — ditto
    [InlineData("42P01")] // undefined_table
    [InlineData("23503")] // foreign_key_violation
    [InlineData(null)]
    public void unrelated_sqlstates_are_not_transient(string? sqlState)
    {
        PostgresqlMigrator.IsTransientCatalogConcurrency(sqlState, "tuple concurrently updated")
            .ShouldBeFalse();
    }

    // Reopen-on-retry behavior: a transient PostgreSQL error can move the
    // Npgsql connection to Closed or Broken. The retry has to put it back to
    // Open before re-invoking the command; otherwise the next call throws
    // "Connection is not open" (an InvalidOperationException, NOT a
    // PostgresException — so the retry filter wouldn't catch it again).

    [Fact]
    public async Task ensure_connection_open_is_a_noop_when_already_open()
    {
        var conn = new RetryStubConnection(ConnectionState.Open);
        var cmd = new RetryStubCommand(conn);

        await PostgresqlMigrator.EnsureConnectionOpenAsync(cmd, CancellationToken.None);

        conn.OpenCallCount.ShouldBe(0);
        conn.CloseCallCount.ShouldBe(0);
    }

    [Fact]
    public async Task ensure_connection_open_reopens_a_closed_connection()
    {
        var conn = new RetryStubConnection(ConnectionState.Closed);
        var cmd = new RetryStubCommand(conn);

        await PostgresqlMigrator.EnsureConnectionOpenAsync(cmd, CancellationToken.None);

        conn.OpenCallCount.ShouldBe(1);
        conn.CloseCallCount.ShouldBe(0); // No need to close before open
    }

    [Fact]
    public async Task ensure_connection_open_closes_then_reopens_a_broken_connection()
    {
        // OpenAsync on a Broken connection throws — must Close first.
        var conn = new RetryStubConnection(ConnectionState.Broken);
        var cmd = new RetryStubCommand(conn);

        await PostgresqlMigrator.EnsureConnectionOpenAsync(cmd, CancellationToken.None);

        conn.CloseCallCount.ShouldBe(1);
        conn.OpenCallCount.ShouldBe(1);
        conn.LastOperationOrder.ShouldBe(new[] { "Close", "Open" });
    }

    [Fact]
    public async Task ensure_connection_open_does_nothing_when_command_has_no_connection()
    {
        var cmd = new RetryStubCommand(connection: null);

        // No throw — the helper short-circuits on a null Connection
        // (DbCommand.Connection is nullable; defensive guard against
        // pathological callers).
        await PostgresqlMigrator.EnsureConnectionOpenAsync(cmd, CancellationToken.None);
    }

    #region Test stubs — minimal DbConnection / DbCommand mocks for the reopen-rules test

    private sealed class RetryStubConnection: DbConnection
    {
        private ConnectionState _state;
        public int OpenCallCount { get; private set; }
        public int CloseCallCount { get; private set; }
        public System.Collections.Generic.List<string> LastOperationOrder { get; } = new();

        public RetryStubConnection(ConnectionState initialState)
        {
            _state = initialState;
        }

        public override ConnectionState State => _state;

        public override void Open()
        {
            OpenCallCount++;
            LastOperationOrder.Add("Open");
            _state = ConnectionState.Open;
        }

        public override void Close()
        {
            CloseCallCount++;
            LastOperationOrder.Add("Close");
            _state = ConnectionState.Closed;
        }

        // The rest of DbConnection — none of these are exercised by
        // EnsureConnectionOpenAsync, throw if anything calls them so a
        // future change to that helper that reaches further into the
        // connection surface fails loud rather than silently.
        public override string ConnectionString { get; set; } = string.Empty;
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override void ChangeDatabase(string databaseName) => throw new System.NotSupportedException();
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new System.NotSupportedException();
        protected override DbCommand CreateDbCommand() => throw new System.NotSupportedException();
    }

    private sealed class RetryStubCommand: DbCommand
    {
        public RetryStubCommand(DbConnection? connection)
        {
            DbConnection = connection;
        }

        protected override DbConnection? DbConnection { get; set; }
        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbParameterCollection DbParameterCollection => throw new System.NotSupportedException();
        protected override DbTransaction? DbTransaction { get; set; }

        public override void Cancel() => throw new System.NotSupportedException();
        public override int ExecuteNonQuery() => throw new System.NotSupportedException();
        public override object? ExecuteScalar() => throw new System.NotSupportedException();
        public override void Prepare() => throw new System.NotSupportedException();
        protected override DbParameter CreateDbParameter() => throw new System.NotSupportedException();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new System.NotSupportedException();
    }

    #endregion
}
