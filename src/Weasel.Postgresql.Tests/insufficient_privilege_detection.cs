using System;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     weasel#598. PostgreSQL reports every privilege refusal as <c>42501</c>, whatever the object
///     was -- "permission denied for database", "permission denied for schema", "must be owner of
///     table". Nothing else may be claimed as a permission failure, because a mistranslation hides
///     the real error behind a remedy that will not help.
/// </summary>
public class insufficient_privilege_detection
{
    private static PostgresException ErrorWith(string sqlState)
        => new("permission denied", "ERROR", "ERROR", sqlState);

    [Fact]
    public void recognises_42501()
    {
        new PostgresqlMigrator()
            .IsInsufficientPrivilege(ErrorWith(PostgresErrorCodes.InsufficientPrivilege))
            .ShouldBeTrue();
    }

    [Fact]
    public void does_not_claim_an_undefined_table_or_a_syntax_error()
    {
        var migrator = new PostgresqlMigrator();

        migrator.IsInsufficientPrivilege(ErrorWith(PostgresErrorCodes.UndefinedTable)).ShouldBeFalse();
        migrator.IsInsufficientPrivilege(ErrorWith(PostgresErrorCodes.SyntaxError)).ShouldBeFalse();
        migrator.IsInsufficientPrivilege(new Exception("boom")).ShouldBeFalse();
    }

    /// <summary>
    ///     Npgsql resolves the login even when the connection was built from a data source rather
    ///     than a hand-written connection string, which is what the generic connection-string read
    ///     on the base class cannot do.
    /// </summary>
    [Fact]
    public void names_the_role_from_the_connection()
    {
        using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);

        new PostgresqlMigrator().RoleFor(conn)
            .ShouldBe(new NpgsqlConnectionStringBuilder(ConnectionSource.ConnectionString).Username);
    }
}
