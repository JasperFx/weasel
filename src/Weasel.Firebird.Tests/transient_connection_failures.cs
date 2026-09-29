using FirebirdSql.Data.FirebirdClient;
using Shouldly;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     weasel#356 for Firebird (A7): which failures are worth a retry after a backoff. The numbers
///     decide, never the SQLSTATE class -- a missing database file is 08001, the same class as a server
///     that is down, and retrying it only delays the real error.
/// </summary>
public class transient_connection_failures: IntegrationContext
{
    private readonly FirebirdMigrator theMigrator = new();

    private static async Task<FbException> openFailureAsync(string connectionString)
        => await Should.ThrowAsync<FbException>(async () =>
        {
            await using var conn = new FbConnection(connectionString);
            await conn.OpenAsync();
        });

    [Fact]
    public async Task a_server_that_is_not_listening_is_transient()
    {
        var nowhere = new FbConnectionStringBuilder(ConnectionString) { Port = 1, Pooling = false, ConnectionTimeout = 5 };

        var ex = await openFailureAsync(nowhere.ConnectionString);

        theMigrator.IsTransientConnectionFailure(ex).ShouldBeTrue(ex.Message);
    }

    /// <summary>
    ///     What a pooled connection reports once the server has dropped it -- a restart, an administrator
    ///     shutting the attachment down.
    /// </summary>
    [Fact]
    public async Task an_attachment_the_server_shut_down_is_transient()
    {
        await using var victim = new FbConnection(new FbConnectionStringBuilder(ConnectionString) { Pooling = false }.ConnectionString);
        await victim.OpenAsync();

        await using (var id = victim.CreateCommand("SELECT CURRENT_CONNECTION FROM RDB$DATABASE"))
        {
            var attachment = Convert.ToInt64(await id.ExecuteScalarAsync());
            await ExecuteAsync($"DELETE FROM MON$ATTACHMENTS WHERE MON$ATTACHMENT_ID = {attachment}");
        }

        var ex = await Should.ThrowAsync<FbException>(async () =>
        {
            await using var cmd = victim.CreateCommand("SELECT 1 FROM RDB$DATABASE");
            await cmd.ExecuteScalarAsync();
        });

        theMigrator.IsTransientConnectionFailure(ex).ShouldBeTrue(ex.Message);
    }

    [Fact]
    public async Task a_missing_database_file_is_not_transient()
    {
        var ex = await openFailureAsync(ConnectionSource.ForDatabase("there_is_no_such_database"));

        ex.SQLSTATE.ShouldStartWith("08", customMessage: "the reason the SQLSTATE class cannot decide");
        theMigrator.IsTransientConnectionFailure(ex).ShouldBeFalse(ex.Message);
    }

    [Fact]
    public async Task a_wrong_password_is_not_transient()
    {
        var ex = await openFailureAsync(
            new FbConnectionStringBuilder(ConnectionString) { Password = "not-the-password", Pooling = false }.ConnectionString);

        theMigrator.IsTransientConnectionFailure(ex).ShouldBeFalse(ex.Message);
    }

    [Fact]
    public async Task a_failed_statement_is_not_transient()
    {
        var ex = await Should.ThrowAsync<FbException>(() => ExecuteAsync("SELECT nope FROM RDB$DATABASE"));

        theMigrator.IsTransientConnectionFailure(ex).ShouldBeFalse(ex.Message);
    }

    [Fact]
    public void a_client_timeout_is_transient_wherever_it_is_in_the_chain()
    {
        theMigrator.IsTransientConnectionFailure(new TimeoutException()).ShouldBeTrue();
        theMigrator.IsTransientConnectionFailure(new InvalidOperationException("wrapped", new TimeoutException()))
            .ShouldBeTrue();
    }

    [Fact]
    public void something_else_is_not_transient()
    {
        theMigrator.IsTransientConnectionFailure(new InvalidOperationException("boom")).ShouldBeFalse();
    }
}
