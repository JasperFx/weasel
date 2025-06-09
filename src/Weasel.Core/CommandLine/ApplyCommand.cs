using JasperFx.CommandLine;
using Spectre.Console;
using Weasel.Core;
using Weasel.Core.CommandLine;

namespace Weasel.CommandLine;

[Description("Applies all outstanding changes to the database(s) based on the current configuration",
    Name = "db-apply")]
public class ApplyCommand: JasperFxAsyncCommand<WeaselInput>
{
    public override async Task<bool> Execute(WeaselInput input)
    {
        JasperFxEnvironment.RunQuiet = true;

        AnsiConsole.Write(
            new FigletText("Weasel"){Justification = Justify.Left});

        using var host = input.BuildHost();

        var databases = await input.FilterDatabases(host).ConfigureAwait(false);

        if (!databases.Any())
        {
            AnsiConsole.MarkupLine("[yellow]No matching databases found, if you were trying to filter databases, use `db-list` command to see the identities");
            return true;
        }

        foreach (var database in databases)
        {
            var descriptor = database.Describe();

            // TODO -- it'd be cool to get a rundown of everything that changed.
            var difference = await database.ApplyAllConfiguredChangesToDatabaseAsync().ConfigureAwait(false);
            switch (difference)
            {
                case SchemaPatchDifference.None:
                    AnsiConsole.MarkupLine($"[gray]No changes detected for DatabaseUri {descriptor.DatabaseUri()} with SubjectUri {descriptor.SubjectUri}.[/]");
                    break;

                case SchemaPatchDifference.Create:
                case SchemaPatchDifference.Update:
                    AnsiConsole.MarkupLine(
                        $"[bold green]Successfully applied migrations for DatabaseUri {descriptor.DatabaseUri()} with SubjectUri {descriptor.SubjectUri}.[/]");
                    break;
            }
        }

        return true;
    }
}
