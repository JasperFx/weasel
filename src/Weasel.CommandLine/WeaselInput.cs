using Baseline;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Oakton;
using Spectre.Console;
using Weasel.Core.Migrations;

namespace Weasel.CommandLine;

public class WeaselInput: NetCoreInput
{
    [Description("Identify which database to dump in the case of multiple databases")]
    public string DatabaseFlag { get; set; }

    [Description("Optionally choose the database interactively")]
    public bool InteractiveFlag { get; set; }

    public IEnumerable<IDatabase> FilterDatabases(IHost host)
    {
        var sources = host.Services.GetServices<IDatabaseSource>();
        var databases = host.Services.GetServices<IDatabase>().Concat(sources.SelectMany(x => x.BuildDatabases())).ToArray();
        if (!databases.Any())
        {
            throw new InvalidOperationException("No Weasel databases were registered in this application");
        }

        if (InteractiveFlag)
        {
            var names = SelectOptions(databases);

            foreach (var database in databases.Where(x => names.Contains(x.Identifier)))
            {
                yield return database;
            }

            yield break;

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

            yield return database;
        }
        else
        {
            foreach (var database in databases)
            {
                yield return database;
            }
        }

    }

    public virtual List<string> SelectOptions(IDatabase[] databases)
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

    public bool TryChooseSingleDatabase(IHost host, out IDatabase database)
    {
        var databases = host.Services.GetServices<IDatabase>().ToArray();

        if (!databases.Any())
        {
            AnsiConsole.MarkupLine("[red]No Weasel databases were registered in this application[/]");
            database = null;
            return false;
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
            database = databases.FirstOrDefault(x => x.Identifier.EqualsIgnoreCase(DatabaseFlag));
            if (database == null)
            {
                AnsiConsole.MarkupLine($"[red]No matching database named '{DatabaseFlag}'[/].");
                listDatabases(databases);

                return false;
            }

            return true;
        }
        else if (databases.Length == 1)
        {
            database = databases.Single();
            return true;
        }
        else
        {
            AnsiConsole.MarkupLine("[bold]A specific database from this list must be selected with the -d|--database flag:[/]");
            listDatabases(databases);

            database = null;
            return false;
        }

        return false;
    }

    private static void listDatabases(IDatabase[] databases)
    {
        var tree = new Tree("Registered Databases");
        foreach (var database1 in databases)
        {
            tree.AddNode(database1.Identifier);
        }

        AnsiConsole.Write(tree);
    }
}
