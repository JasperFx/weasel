using System.Runtime.CompilerServices;

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
    }
}
