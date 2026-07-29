using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
/// Allows targeting test at specified minimum and/or maximum version of PG
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class PgVersionTargetedTheory: TheoryAttribute
{
    private string? _minimumVersion;
    private string? _maximumVersion;

    public string? MinimumVersion
    {
        get => _minimumVersion;
        set
        {
            _minimumVersion = value;
            ApplyVersionGate();
        }
    }

    public string? MaximumVersion
    {
        get => _maximumVersion;
        set
        {
            _maximumVersion = value;
            ApplyVersionGate();
        }
    }

    // See the note on PgVersionTargetedFact.ApplyVersionGate.
    private void ApplyVersionGate()
    {
        var reason = PgVersion.SkipReason(_minimumVersion, _maximumVersion);
        if (reason != null)
        {
            Skip = reason;
        }
    }
}

public class PgVersionTargetedTheoryTests
{
    [Theory]
    [InlineData("test1")]
    [InlineData("test2")]
    public void PgVersionTargetedTheory_CanConnectToDatabase(string ignore)
    {
        PgVersion.Current.ShouldNotBe(default);
    }
}
