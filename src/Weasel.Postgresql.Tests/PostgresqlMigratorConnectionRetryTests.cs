using System;
using Npgsql;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     Unit coverage for the transient-connection-failure predicate that lets one-shot migration tooling
///     retry a database that was refused a connection instead of failing the whole job (weasel#356).
///     The classification is a pure function over the SQLSTATE and is fully testable here.
/// </summary>
public class PostgresqlMigratorConnectionRetryTests
{
    [Fact]
    public void too_many_connections_is_transient()
    {
        PostgresqlMigrator.IsTransientConnectionFailure(PostgresErrorCodes.TooManyConnections).ShouldBeTrue();
    }

    [Fact]
    public void configuration_limit_exceeded_is_transient()
    {
        PostgresqlMigrator.IsTransientConnectionFailure(PostgresErrorCodes.ConfigurationLimitExceeded).ShouldBeTrue();
    }

    [Fact]
    public void cannot_connect_now_is_transient()
    {
        PostgresqlMigrator.IsTransientConnectionFailure(PostgresErrorCodes.CannotConnectNow).ShouldBeTrue();
    }

    [Fact]
    public void disk_full_is_not_transient()
    {
        // Also class 53 (insufficient_resources), but retrying will not clear it.
        PostgresqlMigrator.IsTransientConnectionFailure(PostgresErrorCodes.DiskFull).ShouldBeFalse();
    }

    [Fact]
    public void out_of_memory_is_not_transient()
    {
        PostgresqlMigrator.IsTransientConnectionFailure(PostgresErrorCodes.OutOfMemory).ShouldBeFalse();
    }

    [Fact]
    public void syntax_error_is_not_transient()
    {
        PostgresqlMigrator.IsTransientConnectionFailure(PostgresErrorCodes.SyntaxError).ShouldBeFalse();
    }

    [Fact]
    public void unknown_sqlstate_is_not_transient()
    {
        PostgresqlMigrator.IsTransientConnectionFailure(null).ShouldBeFalse();
        PostgresqlMigrator.IsTransientConnectionFailure("").ShouldBeFalse();
    }

    [Fact]
    public void non_postgres_exceptions_are_not_transient()
    {
        new PostgresqlMigrator().IsTransientConnectionFailure(new InvalidOperationException("boom")).ShouldBeFalse();
    }

    [Fact]
    public void a_bare_too_many_connections_is_transient()
    {
        new PostgresqlMigrator().IsTransientConnectionFailure(tooManyConnections()).ShouldBeTrue();
    }

    [Fact]
    public void a_wrapped_too_many_connections_is_transient()
    {
        // Npgsql commonly delivers a connect-time failure as an outer exception wrapping the
        // PostgresException that carries the SQLSTATE.
        var wrapped = new NpgsqlException("Failed to connect", tooManyConnections());

        new PostgresqlMigrator().IsTransientConnectionFailure(wrapped).ShouldBeTrue();
    }

    [Fact]
    public void an_aggregated_too_many_connections_is_transient()
    {
        // The shape an NpgsqlMultiHostDataSource produces when every host refuses.
        var aggregate = new AggregateException(tooManyConnections(), tooManyConnections());

        new PostgresqlMigrator().IsTransientConnectionFailure(aggregate).ShouldBeTrue();
    }

    [Fact]
    public void a_wrapped_non_transient_postgres_error_is_not_transient()
    {
        var wrapped = new NpgsqlException("boom",
            new PostgresException("syntax error", "ERROR", "ERROR", PostgresErrorCodes.SyntaxError));

        new PostgresqlMigrator().IsTransientConnectionFailure(wrapped).ShouldBeFalse();
    }

    private static PostgresException tooManyConnections() =>
        new("sorry, too many clients already", "FATAL", "FATAL", PostgresErrorCodes.TooManyConnections);
}
