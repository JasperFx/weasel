using FirebirdSql.Data.FirebirdClient;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     weasel#598 for Firebird (A7). A denied read, write, ALTER or DROP carries 335544352; a denied
///     CREATE carries 335545094 on Firebird 3 and 335545264 on 4 and 5, and never 335544352. Neither
///     is necessarily the exception's own code, so every number in the error collection is read.
/// </summary>
public class insufficient_privilege_detection: IntegrationContext
{
    private const string LimitedUser = "WEASEL_LIMITED";
    private const string LimitedPassword = "L1m1ted!";

    [Theory]
    [InlineData(335544352)] // no permission for ... access
    [InlineData(335545094)] // no permission for CREATE (Firebird 3)
    [InlineData(335545264)] // no permission for CREATE (Firebird 4 and 5)
    public void recognises_the_permission_errors(int number)
    {
        FirebirdMigrator.IsPermissionErrorNumber(number).ShouldBeTrue();
    }

    [Theory]
    [InlineData(335544351)] // unsuccessful metadata update -- the wrapper, not the reason
    [InlineData(335544472)] // bad user name or password: a failed login, not a refused statement
    [InlineData(335544569)] // dynamic SQL error
    public void does_not_claim_anything_else(int number)
    {
        FirebirdMigrator.IsPermissionErrorNumber(number).ShouldBeFalse();
    }

    [Fact]
    public void something_that_is_not_a_firebird_error_is_not_a_permission_refusal()
    {
        new FirebirdMigrator().IsInsufficientPrivilege(new InvalidOperationException()).ShouldBeFalse();
    }

    private async Task<FbConnection> openAsLimitedUserAsync()
    {
        await ExecuteAsync($"CREATE OR ALTER USER {LimitedUser} PASSWORD '{LimitedPassword}'");

        var builder = new FbConnectionStringBuilder(ConnectionString) { UserID = LimitedUser, Password = LimitedPassword };
        var conn = new FbConnection(builder.ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task a_denied_create_surfaces_as_insufficient_privilege()
    {
        await using var limited = await openAsLimitedUserAsync();

        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();

        var migrator = new FirebirdMigrator();
        var migration = await SchemaMigration.DetermineAsync(limited, migrator, CancellationToken.None, table);

        var ex = await Should.ThrowAsync<InsufficientDatabasePrivilegeException>(() =>
            migrator.ApplyAllAsync(limited, migration, AutoCreate.CreateOrUpdate));

        ex.Role.ShouldBe(LimitedUser);
        ex.InnerException.ShouldBeOfType<FbException>();
        FirebirdMigrator.HasErrorNumber((FbException)ex.InnerException!, 335545094, 335545264).ShouldBeTrue();
    }

    [Fact]
    public async Task a_denied_read_or_alter_is_a_permission_refusal()
    {
        await ExecuteAsync("CREATE TABLE secrets (id INTEGER)");
        await using var limited = await openAsLimitedUserAsync();
        var migrator = new FirebirdMigrator();

        var read = await Should.ThrowAsync<FbException>(async () =>
        {
            await using var cmd = limited.CreateCommand("SELECT COUNT(*) FROM secrets");
            await cmd.ExecuteScalarAsync();
        });
        migrator.IsInsufficientPrivilege(read).ShouldBeTrue(read.Message);

        // Run the way a migration runs it, which translates the refusal.
        var alter = await Should.ThrowAsync<InsufficientDatabasePrivilegeException>(() =>
            migrator.ExecuteScriptAsync(limited, "ALTER TABLE secrets ADD extra INTEGER"));
        migrator.IsInsufficientPrivilege(alter.InnerException!).ShouldBeTrue(alter.InnerException!.Message);
        FirebirdMigrator.HasErrorNumber((FbException)alter.InnerException!, 335544352).ShouldBeTrue();
    }

    [Fact]
    public async Task a_failure_that_is_not_about_privilege_is_left_as_it_is()
    {
        var ex = await Should.ThrowAsync<FbException>(() => ExecuteAsync("CREATE TABLE broken (id NOT_A_TYPE)"));

        new FirebirdMigrator().IsInsufficientPrivilege(ex).ShouldBeFalse();
    }
}
