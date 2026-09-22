using Shouldly;
using Xunit;

namespace Weasel.Oracle.Tests;

/// <summary>
///     weasel#598. See the SQL Server twin for why the number set is asserted directly.
/// </summary>
public class insufficient_privilege_detection
{
    [Theory]
    [InlineData(1031)] // ORA-01031: insufficient privileges
    [InlineData(1950)] // ORA-01950: no privileges on tablespace
    public void recognises_the_permission_errors(int number)
    {
        OracleMigrator.IsPermissionErrorNumber(number).ShouldBeTrue();
    }

    /// <summary>
    ///     ORA-00942 is deliberately absent: Oracle uses it for both a missing object and an
    ///     invisible one, and calling a genuinely missing table a permission failure would send the
    ///     reader off to check grants that are fine.
    /// </summary>
    [Theory]
    [InlineData(942)]  // table or view does not exist
    [InlineData(955)]  // name is already used by an existing object
    [InlineData(1017)] // invalid username/password -- a failed login
    public void does_not_claim_anything_else(int number)
    {
        OracleMigrator.IsPermissionErrorNumber(number).ShouldBeFalse();
    }
}
