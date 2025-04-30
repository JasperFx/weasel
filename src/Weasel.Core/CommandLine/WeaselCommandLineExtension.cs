using JasperFx.CommandLine;
using JasperFx.Environment;
using JasperFx.Resources;
using Microsoft.Extensions.DependencyInjection;

namespace Weasel.Core.CommandLine;

internal class WeaselCommandLineExtension : IServiceRegistrations
{
    public void Configure(IServiceCollection services)
    {
        services.AddSingleton<IEnvironmentCheckFactory, DatabaseConnectionCheck>();
        services.AddSingleton<IStatefulResourceSource, DatabaseResources>();
    }
}
