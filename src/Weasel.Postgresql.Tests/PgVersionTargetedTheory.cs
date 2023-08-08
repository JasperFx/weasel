using Npgsql;
using Shouldly;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Weasel.Postgresql.Tests;

/// <summary>
/// Allows targeting test at specified minimum and/or maximum version of PG
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
[XunitTestCaseDiscoverer("Marten.Testing.Harness.PgVersionTargetedTheoryDiscoverer", "Marten.Testing")]
public sealed class PgVersionTargetedTheory: TheoryAttribute
{
    public string MinimumVersion { get; set; }
    public string MaximumVersion { get; set; }
}

public sealed class PgVersionTargetedTheoryDiscoverer: TheoryDiscoverer
{
    internal static readonly Version Version;

    static PgVersionTargetedTheoryDiscoverer()
    {
        // PG version does not change during test run so we can do static ctor
        using var c = new NpgsqlConnection(ConnectionSource.ConnectionString);
        c.Open();
        Version = c.PostgreSqlVersion;
        c.Close();
    }

    public PgVersionTargetedTheoryDiscoverer(IMessageSink diagnosticMessageSink): base(diagnosticMessageSink)
    {
    }

    public override IEnumerable<IXunitTestCase> Discover(ITestFrameworkDiscoveryOptions discoveryOptions,
        ITestMethod testMethod,
        IAttributeInfo theoryAttribute)
    {
        var minimumVersion = theoryAttribute.GetNamedArgument<string>(nameof(PgVersionTargetedTheory.MinimumVersion));
        var maximumVersion = theoryAttribute.GetNamedArgument<string>(nameof(PgVersionTargetedTheory.MaximumVersion));

        if (minimumVersion != null && Version.TryParse(minimumVersion, out var minVersion) && Version < minVersion)
        {
            return new[]
            {
                new TestCaseSkippedDueToVersion(
                    $"Minimum required PG version {minimumVersion} is higher than {Version}", DiagnosticMessageSink,
                    discoveryOptions.MethodDisplayOrDefault(), discoveryOptions.MethodDisplayOptionsOrDefault(),
                    testMethod)
            };
        }

        if (maximumVersion != null && Version.TryParse(maximumVersion, out var maxVersion) && Version > maxVersion)
        {
            return new[]
            {
                new TestCaseSkippedDueToVersion(
                    $"Maximum allowed PG version {maximumVersion} is higher than {Version}", DiagnosticMessageSink,
                    discoveryOptions.MethodDisplayOrDefault(), discoveryOptions.MethodDisplayOptionsOrDefault(),
                    testMethod)
            };
        }

        return CreateTestCasesForTheory(discoveryOptions, testMethod, theoryAttribute);
    }

    internal sealed class TestCaseSkippedDueToVersion: XunitTestCase
    {
        [Obsolete("Called by the de-serializer", true)]
        public TestCaseSkippedDueToVersion()
        {
        }

        public TestCaseSkippedDueToVersion(string skipReason, IMessageSink diagnosticMessageSink,
            TestMethodDisplay defaultMethodDisplay, TestMethodDisplayOptions defaultMethodDisplayOptions,
            ITestMethod testMethod, object[] testMethodArguments = null): base(diagnosticMessageSink,
            defaultMethodDisplay, defaultMethodDisplayOptions, testMethod, testMethodArguments)
        {
            SkipReason = skipReason;
        }
    }
}

public class PgVersionTargetedTheoryDiscovererTests
{
    [Theory]
    [InlineData("test1")]
    [InlineData("test2")]
    public void PgVersionTargetedTheoryDiscoverer_CanConnectToDatabase(string ignore)
    {
        PgVersionTargetedTheoryDiscoverer.Version.ShouldNotBe(default);
    }
}
