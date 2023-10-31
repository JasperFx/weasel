using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Functions;
using Xunit;

namespace Weasel.SqlServer.Tests.Functions;

public class FunctionBodyTests
{
    private string theFunctionBody = @"
CREATE OR ALTER FUNCTION public.sample(
    @version uniqueidentifier)
  RETURNS uniqueidentifier AS
BEGIN
RETURN @version;
END;";

    [Fact]
    public void derive_the_function_identifier_from_the_body()
    {
        var func = new FunctionBody(new SqlServerObjectName("public", "sample"), new string[0], theFunctionBody);
        Function.ParseIdentifier(func.Body).ShouldBe(new SqlServerObjectName("public", "sample"));
    }
}
