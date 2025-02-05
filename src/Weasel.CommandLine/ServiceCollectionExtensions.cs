
using Microsoft.Extensions.DependencyInjection;
using JasperFx.Environment;

namespace Weasel.CommandLine;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Add an environment check to assert that all the known Weasel databases
    /// match the known configuration. Do note that connection checks are automatically added
    /// to your application
    /// </summary>
    /// <param name="services"></param>
    public static void CheckAllWeaselDatabases(this IServiceCollection services)
    {
        services.AddSingleton<IEnvironmentCheckFactory, AssertAllWeaselDatabasesCheck>();
    }
}
