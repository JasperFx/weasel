using JasperFx.CommandLine;
using JasperFx.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Spectre.Console;
using Weasel.Core.Migrations;

namespace Weasel.Core.CommandLine;

public class WeaselInput: NetCoreInput
{
    [Description("Identify which database to dump in the case of multiple databases. Can be a partial Uri match")]
    public string? DatabaseFlag { get; set; }

    public async ValueTask<List<IDatabase>> AllDatabases(IHost host)
    {
        var databases = host.Services.GetServices<IDatabase>().ToList();
        var sources = host.Services.GetServices<IDatabaseSource>();

        foreach (var source in sources)
        {
            var found = await source.BuildDatabases().ConfigureAwait(false);
            databases.AddRange(found);
        }

        return databases;
    }

    public async ValueTask<IList<IDatabase>> FilterDatabases(IHost host)
    {
        var databases = await AllDatabases(host).ConfigureAwait(false);

        if (!databases.Any())
        {
            throw new InvalidOperationException("No Weasel databases were registered in this application");
        }

        IList<IDatabase> filtered = [];

        if (DatabaseFlag.IsNotEmpty())
        {
            if (Uri.TryCreate(DatabaseFlag, UriKind.Absolute, out var uri))
            {
                filtered = databases.Where(x =>
                {
                    var descriptor = x.Describe();

                    return descriptor.SubjectUri.Matches(uri) || descriptor.DatabaseUri().Matches(uri);
                }).ToList();
            }
            else
            {
                filtered = databases.Where(x => x.Identifier.EqualsIgnoreCase(DatabaseFlag)).ToList();
            }
        }

        if (!filtered.Any())
        {
            ListCommand.RenderDatabases(databases);
            throw new InvalidOperationException($"No Weasel databases matched the supplied --database flag '{DatabaseFlag}'");
        }

        return filtered;

    }

    public async ValueTask<(bool, IDatabase?)> TryChooseSingleDatabase(IHost host)
    {
        var databases = host.Services.GetServices<IDatabase>().ToList();
        var sources = host.Services.GetServices<IDatabaseSource>();
        foreach (var source in sources)
        {
            databases.AddRange(await source.BuildDatabases().ConfigureAwait(false));
        }

        if (!databases.Any())
        {
            AnsiConsole.MarkupLine("[red]No Weasel databases were registered in this application[/]");
            return (false, null);
        }

        if (DatabaseFlag.IsNotEmpty())
        {
            if (Uri.TryCreate(DatabaseFlag, UriKind.Absolute, out var uri))
            {
                var first = databases.FirstOrDefault(x =>
                {
                    var descriptor = x.Describe();

                    return descriptor.SubjectUri.Matches(uri) || descriptor.DatabaseUri().Matches(uri);
                });

                if (first != null)
                {
                    return (true, first);
                }
            }

            var database = databases.FirstOrDefault(x => x.Identifier.EqualsIgnoreCase(DatabaseFlag));
            if (database != null)
            {
                return (true, database);
            }

            AnsiConsole.MarkupLine($"[red]No matching database with either subject or database Uri matching '{DatabaseFlag}'[/].");
            ListCommand.RenderDatabases(databases);

            return (false, null);

        }

        if (databases.Count == 1)
        {
            return (true, databases.Single());
        }

        AnsiConsole.MarkupLine("[bold]A specific database from this list must be selected with the -d|--database flag:[/]");
        ListCommand.RenderDatabases(databases);

        return (false, null);
    }
}
