using System.Runtime.CompilerServices;
using Npgsql;

namespace Weasel.Postgresql.Tests;

/// <summary>
/// The Postgres CI matrix runs the whole suite twice, once with case-sensitive
/// qualified names and once without, selected by an environment variable.
/// </summary>
/// <remarks>
/// This was a custom xUnit v2 TestFramework. v3 reshaped that extensibility point,
/// and a module initializer is a better fit anyway: it needs no test-framework hook,
/// and it runs before discovery rather than alongside it.
/// </remarks>
internal static class TestSetup
{
    [ModuleInitializer]
    internal static void Initialize()
    {
        if (bool.TryParse(
                Environment.GetEnvironmentVariable("USE_CASE_SENSITIVE_QUALIFIED_NAMES"),
                out var useCaseSensitiveQualifiedNames)
           )
        {
            PostgresqlProvider.Instance.UseCaseSensitiveQualifiedNames = useCaseSensitiveQualifiedNames;
        }

        WarmUpNpgsqlTypeInference();
    }

    /// <summary>
    /// Populate Npgsql's process-global type mapper before any test runs.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When a parameter is added without an explicit type, Weasel leaves
    /// <see cref="NpgsqlParameter.NpgsqlDbType" /> unset and lets Npgsql infer it from the
    /// value. That getter answers out of Npgsql's process-global type mapper, which stays
    /// empty until the process constructs its first <see cref="NpgsqlDataSource" /> or
    /// <see cref="NpgsqlConnection" /> with a connection string — until then it reports
    /// <c>NpgsqlDbType.Unknown</c> for every value.
    /// </para>
    /// <para>
    /// So a test asserting an inferred parameter type silently depends on some *other*
    /// test in the same process having built a data source first. Under xUnit v3's parallel
    /// collections that is a race, and it is exactly what made
    /// <c>CommandExtensionsTests.add_first_parameter</c> fail in three of the four Postgres
    /// CI jobs and then pass on an unchanged re-run. See weasel#398.
    /// </para>
    /// <para>
    /// Constructing a builder is enough to seed the global mapper; it opens no sockets and
    /// needs no connection string, so it is safe to do unconditionally at load.
    /// </para>
    /// </remarks>
    private static void WarmUpNpgsqlTypeInference()
    {
        _ = new NpgsqlDataSourceBuilder();
    }
}
