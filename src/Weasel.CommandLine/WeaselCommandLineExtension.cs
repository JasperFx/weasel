using Microsoft.Extensions.DependencyInjection;
using Oakton;
using Oakton.Environment;
using Oakton.Resources;

namespace Weasel.CommandLine;

internal class WeaselCommandLineExtension : IServiceRegistrations
{
    public void Configure(IServiceCollection services)
    {
        services.AddSingleton<IEnvironmentCheckFactory, DatabaseConnectionCheck>();
        services.AddSingleton<IStatefulResourceSource, DatabaseResources>();
    }
}
