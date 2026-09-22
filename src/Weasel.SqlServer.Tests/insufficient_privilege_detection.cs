using Shouldly;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     weasel#598. <see cref="Microsoft.Data.SqlClient.SqlException" /> has no public constructor,
///     so the error-number set is asserted directly. It is the part that rots: a number dropped or
///     a non-permission number added both change which failures get the permission remedy, and
///     neither shows up in a migration test.
/// </summary>
public class insufficient_privilege_detection
{
    [Theory]
    [InlineData(229)]  // permission denied on object
    [InlineData(230)]  // permission denied on column
    [InlineData(262)]  // CREATE SCHEMA denied -- the one in the issue
    [InlineData(297)]  // user does not have permission to perform this action
    [InlineData(300)]  // VIEW permission denied
    [InlineData(15247)] // user does not have permission to perform this action
    public void recognises_the_permission_errors(int number)
    {
        SqlServerMigrator.IsPermissionErrorNumber(number).ShouldBeTrue();
    }

    [Theory]
    [InlineData(208)]   // invalid object name -- a missing table, not a denied one
    [InlineData(2714)]  // there is already an object named ...
    [InlineData(1205)]  // deadlock victim
    [InlineData(0)]
    public void does_not_claim_anything_else(int number)
    {
        SqlServerMigrator.IsPermissionErrorNumber(number).ShouldBeFalse();
    }
}
