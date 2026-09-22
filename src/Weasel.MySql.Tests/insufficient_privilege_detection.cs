using Shouldly;
using Xunit;

namespace Weasel.MySql.Tests;

/// <summary>
///     weasel#598. See the SQL Server twin for why the number set is asserted directly.
/// </summary>
public class insufficient_privilege_detection
{
    [Theory]
    [InlineData(1044)] // access denied for user to database
    [InlineData(1142)] // command denied to user for table
    [InlineData(1143)] // command denied to user for column
    [InlineData(1227)] // access denied; you need a global privilege
    [InlineData(1370)] // execute command denied to user for routine
    public void recognises_the_permission_errors(int number)
    {
        MySqlMigrator.IsPermissionErrorNumber(number).ShouldBeTrue();
    }

    [Theory]
    [InlineData(1045)] // access denied for user (using password) -- a failed login, not a refused statement
    [InlineData(1146)] // table doesn't exist
    [InlineData(1050)] // table already exists
    public void does_not_claim_anything_else(int number)
    {
        MySqlMigrator.IsPermissionErrorNumber(number).ShouldBeFalse();
    }
}
