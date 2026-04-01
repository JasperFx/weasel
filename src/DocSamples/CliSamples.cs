using JasperFx;
using JasperFx.CommandLine;
using Microsoft.AspNetCore.Builder;

namespace DocSamples;

public class CliSamples
{
    public async Task<int> enable_weasel_cli()
    {
        var args = Array.Empty<string>();

        #region sample_cli_enable_weasel_cli
        var builder = WebApplication.CreateBuilder(args);

        // 1. Add this call to enable JasperFx extensions
        builder.Host.ApplyJasperFxExtensions();

        // ... configure services as usual ...

        var app = builder.Build();

        // 2. Replace app.Run() with this:
        return await app.RunJasperFxCommands(args);
        #endregion
    }

    public void auto_start_host_for_testing()
    {
        #region sample_cli_auto_start_host
        JasperFxEnvironment.AutoStartHost = true;
        #endregion
    }
}
