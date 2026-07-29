using Npgsql;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
/// The PostgreSQL server version the suite is running against. Resolved once, since it
/// cannot change during a test run.
/// </summary>
public static class PgVersion
{
    public static readonly Version Current;

    static PgVersion()
    {
        var versionFromEnv = Environment.GetEnvironmentVariable("postgresql_version");
        if (!string.IsNullOrEmpty(versionFromEnv))
        {
            Current = Version.Parse(versionFromEnv);
            return;
        }

        using var c = new NpgsqlConnection(ConnectionSource.ConnectionString);
        c.Open();
        Current = c.PostgreSqlVersion;
        c.Close();
    }

    /// <summary>
    /// The reason a test constrained to this version range should be skipped on the
    /// current server, or null when the server is inside the range.
    /// </summary>
    public static string? SkipReason(string? minimumVersion, string? maximumVersion)
    {
        if (minimumVersion != null && Version.TryParse(minimumVersion, out var minVersion) && Current < minVersion)
        {
            return $"Minimum required PG version {minimumVersion} is higher than {Current}";
        }

        if (maximumVersion != null && Version.TryParse(maximumVersion, out var maxVersion) && Current > maxVersion)
        {
            return $"Maximum allowed PG version {maximumVersion} is higher than {Current}";
        }

        return null;
    }
}

/// <summary>
/// Allows targeting test at specified minimum and/or maximum version of PG
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class PgVersionTargetedFact: FactAttribute
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

    // Replaces the v2 custom discoverer that handed back a pre-skipped XunitTestCase.
    // v3 removed that extensibility point but seals FactAttribute.Skip rather than
    // making it virtual, so the gate is applied as each named argument is assigned -
    // which happens after construction, hence re-running on both setters. v3
    // instantiates the attribute and reads Skip off the instance, so this is seen.
    // Only ever sets Skip, never clears it, so an explicit Skip = "..." survives.
    private void ApplyVersionGate()
    {
        var reason = PgVersion.SkipReason(_minimumVersion, _maximumVersion);
        if (reason != null)
        {
            Skip = reason;
        }
    }
}

public class PgVersionTests
{
    [Fact]
    public void PgVersion_CanConnectToDatabase()
    {
        PgVersion.Current.ShouldNotBe(default);
    }
}
