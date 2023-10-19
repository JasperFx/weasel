using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

[assembly: TestFramework("Weasel.Postgresql.Tests.TestSetup", "Weasel.Postgresql.Tests")]

namespace Weasel.Postgresql.Tests;

public class TestSetup: XunitTestFramework
{
    public TestSetup(IMessageSink messageSink)
        : base(messageSink)
    {
        if (bool.TryParse(
                Environment.GetEnvironmentVariable("USE_CASE_SENSITIVE_QUALIFIED_NAMES"),
                out var useCaseSensitiveQualifiedNames)
           )
        {
            PostgresqlProvider.Instance.UseCaseSensitiveQualifiedNames = useCaseSensitiveQualifiedNames;
        }
    }
}
