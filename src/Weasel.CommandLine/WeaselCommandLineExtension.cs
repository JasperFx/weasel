using Microsoft.Extensions.DependencyInjection;
using JasperFx;
using JasperFx.CommandLine;
using JasperFx.Environment;
using JasperFx.Resources;

namespace Weasel.CommandLine;

internal class WeaselCommandLineExtension : IServiceRegistrations
{
    public void Configure(IServiceCollection services)
    {
        services.AddSingleton<IEnvironmentCheckFactory, DatabaseConnectionCheck>();
        services.AddSingleton<IStatefulResourceSource, DatabaseResources>();
    }
}
