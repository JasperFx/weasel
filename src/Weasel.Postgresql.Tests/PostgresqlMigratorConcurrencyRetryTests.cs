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
}
