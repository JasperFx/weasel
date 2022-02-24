using Baseline;
using Microsoft.Extensions.DependencyInjection;
using Oakton;
using Spectre.Console;
using Weasel.Core;
using Weasel.Core.Migrations;

namespace Weasel.CommandLine;

[Description("Applies all outstanding changes to the database(s) based on the current configuration", Name = "db-apply")]
public class ApplyCommand : OaktonAsyncCommand<WeaselInput>
{
    public override async Task<bool> Execute(WeaselInput input)
    {
        using var host = input.BuildHost();

        var databases = await input.FilterDatabases(host);

        foreach (var database in databases)
        {
            // TODO -- it'd be cool to get a rundown of everything that changed.
            var difference = await database.ApplyAllConfiguredChangesToDatabaseAsync();
            switch (difference)
            {
                case SchemaPatchDifference.None:
                    AnsiConsole.MarkupLine($"[gray]No changes detected for database '{database.Identifier}'.[/]");
                    break;

                case SchemaPatchDifference.Create:
                case SchemaPatchDifference.Update:
                    AnsiConsole.MarkupLine($"[bold green]Successfully applied migrations for '{database.Identifier}'.[/]");
                    break;

            }
        }

        return true;
    }
}
