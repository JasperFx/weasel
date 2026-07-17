using JasperFx.CommandLine;
using Spectre.Console;
using Weasel.Core;
using Weasel.Core.CommandLine;
using Weasel.Core.Migrations;

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

        var total = databases.Count;
        for (var i = 0; i < total; i++)
        {
            var database = databases[i];
            var descriptor = database.Describe();

            // A 512-database walk with no output until it finishes (or fails) is its own small cruelty --
            // an operator watching this needs to see it moving and be able to estimate completion.
            var progress = $"({i + 1}/{total})";

            try
            {
                // TODO -- it'd be cool to get a rundown of everything that changed.
                var difference = await database.ApplyAllConfiguredChangesWithRetriesAsync().ConfigureAwait(false);
                switch (difference)
                {
                    case SchemaPatchDifference.None:
                        AnsiConsole.MarkupLine($"[gray]{progress} No changes detected for DatabaseUri {descriptor.DatabaseUri()} with SubjectUri {descriptor.SubjectUri}.[/]");
                        break;

                    case SchemaPatchDifference.Create:
                    case SchemaPatchDifference.Update:
                        AnsiConsole.MarkupLine(
                            $"[bold green]{progress} Successfully applied migrations for DatabaseUri {descriptor.DatabaseUri()} with SubjectUri {descriptor.SubjectUri}.[/]");
                        break;
                }
            }
            finally
            {
                // Nothing else needs this database's connections once its apply is done, and this command
                // owns its data sources -- there are no application sessions sharing them. Releasing here
                // keeps peak connection usage at ~the one database being applied, instead of trailing an
                // idle pool per database until the connection idle lifetime expires (weasel#356).
                try
                {
                    await database.ReleaseConnectionPoolAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    // Releasing the pool is housekeeping. If it throws while an apply is already failing,
                    // letting it out of the finally would discard the migration exception the operator
                    // actually needs to see.
                    AnsiConsole.MarkupLine(
                        $"[yellow]{progress} Unable to release the connection pool for DatabaseUri {descriptor.DatabaseUri()}: {Markup.Escape(e.Message)}[/]");
                }
            }
        }

        return true;
    }
}
