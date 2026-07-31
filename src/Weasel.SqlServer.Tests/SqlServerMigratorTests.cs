using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests;

public class SqlServerMigratorTests
{
    [Fact]
    public void matches_sql_connection()
    {
        var migrator = new SqlServerMigrator();

        using var connection = new SqlConnection();
        migrator.MatchesConnection(connection).ShouldBeTrue();
    }

    [Fact]
    public void create_table_returns_sql_server_table()
    {
        var migrator = new SqlServerMigrator();
        var identifier = new SqlServerObjectName("dbo", "test_table");

        var table = migrator.CreateTable(identifier);

        table.ShouldBeOfType<Table>();
        table.Identifier.ShouldBe(identifier);
    }

    /// <summary>
    ///     weasel#416. AssertValidIdentifier is the only identifier check in the stack, and this provider's
    ///     was an empty method body until now, so it has to reject the characters that let a name escape the
    ///     statement it is written into. SQL Server delimits identifiers with <c>[...]</c> as well as
    ///     <c>"..."</c>, so <c>]</c> matters alongside <c>"</c>; a <c>'</c> closes a string literal, which is
    ///     where names land in the existence checks (<c>IF OBJECT_ID('...')</c>); a <c>;</c> starts a new
    ///     statement.
    /// </summary>
    [Theory]
    [InlineData("users\"", "a trailing double quote")]
    [InlineData("\"users", "a leading double quote")]
    [InlineData("us\"ers", "an embedded double quote")]
    [InlineData("\"users\"", "a fully quote-wrapped name")]
    [InlineData("users]", "a bracket that closes a delimited identifier")]
    [InlineData("[users]", "a fully bracket-wrapped name")]
    [InlineData("us]ers", "an embedded closing bracket")]
    [InlineData("users]; drop table users; --", "a bracket-and-semicolon payload")]
    [InlineData("users'", "a trailing single quote")]
    [InlineData("us'ers", "an embedded single quote")]
    [InlineData("users'); drop table users; --", "a quote-and-semicolon payload")]
    [InlineData("users;", "a trailing semicolon")]
    [InlineData("us;ers", "an embedded semicolon")]
    public void assert_identifier_rejects_quote_bracket_and_semicolon(string name, string description)
    {
        var migrator = new SqlServerMigrator();

        Should.Throw<InvalidOperationException>(
            () => migrator.AssertValidIdentifier(name),
            $"Expected {description} to be rejected");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("us ers")]
    [InlineData("us\ters")]
    [InlineData("us\ners")]
    [InlineData("us\rers")]
    [InlineData("users\n-- the rest of this statement is now a comment")]
    public void assert_identifier_rejects_null_empty_and_whitespace(string? name)
    {
        var migrator = new SqlServerMigrator();

        Should.Throw<InvalidOperationException>(() => migrator.AssertValidIdentifier(name!));
    }

    [Fact]
    public void assert_identifier_rejects_names_past_the_sysname_limit()
    {
        var migrator = new SqlServerMigrator();

        Should.NotThrow(() => migrator.AssertValidIdentifier(new string('a', 128)));
        Should.Throw<InvalidOperationException>(() => migrator.AssertValidIdentifier(new string('a', 129)));
    }

    [Fact]
    public void invalid_identifier_message_says_which_rule_was_broken()
    {
        var migrator = new SqlServerMigrator();

        var ex = Should.Throw<InvalidOperationException>(() => migrator.AssertValidIdentifier("us]ers"));

        ex.Message.ShouldContain("us]ers");
        ex.Message.ShouldContain("closing square bracket");
    }

    /// <summary>
    ///     Names Weasel and its consumers actually generate must keep working -- the tightening is aimed at
    ///     a handful of characters, not at narrowing the identifier grammar.
    /// </summary>
    [Theory]
    [InlineData("mt_doc_user")]
    [InlineData("mt_stream")]
    [InlineData("mt_doc_user_hilo")]
    [InlineData("users$1")]
    [InlineData("_leading_underscore")]
    [InlineData("MixedCaseName")]
    [InlineData("naïve_café")]
    [InlineData("table-with-dashes")]
    [InlineData("#temp_table")]
    [InlineData("mt_doc_target.p_tenant_one")]
    public void assert_identifier_still_accepts_ordinary_names(string name)
    {
        Should.NotThrow(() => new SqlServerMigrator().AssertValidIdentifier(name));
    }

    [Fact]
    public async Task can_ensure_database_that_does_not_exist()
    {
        var migrator = new SqlServerMigrator();
        var databaseName = $"weasel_ensure_{Guid.NewGuid():N}";

        var builder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = databaseName
        };

        try
        {
            await using var targetConn = new SqlConnection(builder.ConnectionString);
            await migrator.EnsureDatabaseExistsAsync(targetConn);

            // Verify the database was created by opening a connection to it
            await using var verifyConn = new SqlConnection(builder.ConnectionString);
            await verifyConn.OpenAsync();
        }
        finally
        {
            var adminBuilder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
            {
                InitialCatalog = "master"
            };
            await using var adminConn = new SqlConnection(adminBuilder.ConnectionString);
            await adminConn.OpenAsync();

            var cmd = adminConn.CreateCommand();
            cmd.CommandText = $@"
                IF DB_ID('{databaseName}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{databaseName}];
                END";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task ensure_database_is_idempotent()
    {
        var migrator = new SqlServerMigrator();

        // Ensure the connection string has an Initial Catalog (CI may omit it)
        var builder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = "master"
        };

        // Use the existing master database - should not throw
        await using var connection = new SqlConnection(builder.ConnectionString);
        await migrator.EnsureDatabaseExistsAsync(connection);
    }

    /// <summary>
    ///     weasel#415. Several callers provisioning the same database at once used to leave the losers of
    ///     the check-then-create race holding SqlException 1801. Against 9.22.0 this fails.
    /// </summary>
    [Fact]
    public async Task ensure_database_is_safe_under_concurrent_callers()
    {
        var databaseName = $"weasel_ensure_race_{Guid.NewGuid():N}";

        var builder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = databaseName
        };

        try
        {
            // All of these see DB_ID() return null and race into CREATE DATABASE together.
            var attempts = Enumerable.Range(0, 8).Select(async _ =>
            {
                await using var conn = new SqlConnection(builder.ConnectionString);
                await new SqlServerMigrator().EnsureDatabaseExistsAsync(conn);
            });

            await Task.WhenAll(attempts);

            // Every caller must be able to take the postcondition at face value: the database exists
            // and accepts a connection by the time EnsureDatabaseExistsAsync returns.
            await using var verifyConn = new SqlConnection(builder.ConnectionString);
            await verifyConn.OpenAsync();
        }
        finally
        {
            await dropDatabaseAsync(databaseName);
        }
    }

    /// <summary>
    ///     A database name carrying a ']' would otherwise close the delimited identifier early.
    /// </summary>
    [Fact]
    public async Task ensure_database_escapes_a_bracket_in_the_database_name()
    {
        var databaseName = $"weasel_ensure_]_{Guid.NewGuid():N}";

        var builder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = databaseName
        };

        try
        {
            await using var conn = new SqlConnection(builder.ConnectionString);
            await new SqlServerMigrator().EnsureDatabaseExistsAsync(conn);

            await using var verifyConn = new SqlConnection(builder.ConnectionString);
            await verifyConn.OpenAsync();
        }
        finally
        {
            await dropDatabaseAsync(databaseName);
        }
    }

    /// <summary>
    ///     The wait for the database to come online has to end in a clear failure rather than in silence
    ///     or a hang. An offline database is the reproducible stand-in for "created, but still refusing
    ///     logins": DB_ID() reports it, so nothing is created, and no connection to it will ever succeed.
    /// </summary>
    [Fact]
    public async Task times_out_with_a_clear_message_when_the_database_never_accepts_connections()
    {
        var databaseName = $"weasel_ensure_offline_{Guid.NewGuid():N}";

        var builder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = databaseName
        };

        try
        {
            await using (var conn = new SqlConnection(builder.ConnectionString))
            {
                await new SqlServerMigrator().EnsureDatabaseExistsAsync(conn);
            }

            await executeAgainstMasterAsync(
                $"ALTER DATABASE [{databaseName}] SET OFFLINE WITH ROLLBACK IMMEDIATE;");

            var migrator = new SqlServerMigrator
            {
                DatabaseAvailabilityTimeout = 1.Seconds(), DatabaseAvailabilityPollingInterval = 100.Milliseconds()
            };

            await using var offlineConn = new SqlConnection(builder.ConnectionString);

            var ex = await Should.ThrowAsync<TimeoutException>(async () =>
                await migrator.EnsureDatabaseExistsAsync(offlineConn));

            ex.Message.ShouldContain(databaseName);
            ex.Message.ShouldContain(nameof(SqlServerMigrator.DatabaseAvailabilityTimeout));
            ex.InnerException.ShouldBeOfType<SqlException>();
        }
        finally
        {
            await executeAgainstMasterAsync($"ALTER DATABASE [{databaseName}] SET ONLINE;");
            await dropDatabaseAsync(databaseName);
        }
    }

    private static async Task executeAgainstMasterAsync(string sql)
    {
        var adminBuilder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = "master"
        };
        await using var adminConn = new SqlConnection(adminBuilder.ConnectionString);
        await adminConn.OpenAsync();

        var cmd = adminConn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task dropDatabaseAsync(string databaseName)
    {
        var adminBuilder = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            InitialCatalog = "master"
        };
        await using var adminConn = new SqlConnection(adminBuilder.ConnectionString);
        await adminConn.OpenAsync();

        var cmd = adminConn.CreateCommand();
        cmd.CommandText = $@"
            IF DB_ID(@name) IS NOT NULL
            BEGIN
                ALTER DATABASE [{databaseName.Replace("]", "]]")}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{databaseName.Replace("]", "]]")}];
            END";
        var param = cmd.CreateParameter();
        param.ParameterName = "@name";
        param.Value = databaseName;
        cmd.Parameters.Add(param);
        await cmd.ExecuteNonQueryAsync();
    }
}
