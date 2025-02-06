using JasperFx.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using JasperFx;
using JasperFx.CommandLine;
using Spectre.Console;
using Weasel.Core.Migrations;

namespace Weasel.CommandLine;

public class WeaselInput: NetCoreInput
{
    [Description("Identify which database to dump in the case of multiple databases")]
    public string? DatabaseFlag { get; set; }

    [Description("Optionally choose the database interactively")]
    public bool InteractiveFlag { get; set; }

    public async ValueTask<IList<IDatabase>> FilterDatabases(IHost host)
    {
        var databases = host.Services.GetServices<IDatabase>().ToList();
        var sources = host.Services.GetServices<IDatabaseSource>();

        foreach (var source in sources)
        {
            var found = await source.BuildDatabases().ConfigureAwait(false);
            databases.AddRange(found);
        }

        if (!databases.Any())
        {
            throw new InvalidOperationException("No Weasel databases were registered in this application");
        }

        if (InteractiveFlag)
        {
            var names = SelectOptions(databases);

            return databases.Where(x => names.Contains(x.Identifier)).ToList();
        }

        if (DatabaseFlag.IsNotEmpty())
        {
            var database = databases.FirstOrDefault(x => x.Identifier.EqualsIgnoreCase(DatabaseFlag));
            if (database == null)
            {
                AnsiConsole.MarkupLine($"[red]No matching database named '{DatabaseFlag}'[/].");
                listDatabases(databases);

                throw new InvalidOperationException(
                    $"Specified database does not exist. Options are {databases.Select(x => $"'{x.Identifier}'").Join(", ")}");
            }

            return new List<IDatabase> { database };
        }
        else
        {
            return databases;
        }

    }

    public virtual List<string> SelectOptions(List<IDatabase> databases)
    {
        var names = AnsiConsole.Prompt(
            new MultiSelectionPrompt<string>()
                .Title("Which databases?")
                .NotRequired() // Not required to have a favorite fruit
                .PageSize(10)
                .InstructionsText(
                    "[grey](Press [blue]<space>[/] to toggle a database on or off, " +
                    "[green]<enter>[/] to accept)[/]")
                .AddChoices(databases.Select(x => x.Identifier)));
        return names;
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

        if (InteractiveFlag)
        {
            DatabaseFlag = AnsiConsole.Prompt(new SelectionPrompt<string>()
                .Title("Which database?")
                .AddChoices(databases.Select(x => x.Identifier))
            );
        }

        if (DatabaseFlag.IsNotEmpty())
        {
            var database = databases.FirstOrDefault(x => x.Identifier.EqualsIgnoreCase(DatabaseFlag));
            if (database != null)
            {
                return (true, database);
            }

            AnsiConsole.MarkupLine($"[red]No matching database named '{DatabaseFlag}'[/].");
            listDatabases(databases);

            return (false, null);

        }

        if (databases.Count == 1)
        {
            return (true, databases.Single());
        }

        AnsiConsole.MarkupLine("[bold]A specific database from this list must be selected with the -d|--database flag:[/]");
        listDatabases(databases);

        return (false, null);
    }

    private static void listDatabases(List<IDatabase> databases)
    {
        var tree = new Tree("Registered Databases");
        foreach (var database1 in databases)
        {
            tree.AddNode(database1.Identifier);
        }

        AnsiConsole.Write(tree);
    }
}
