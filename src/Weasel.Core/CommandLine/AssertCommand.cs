using JasperFx.CommandLine;
using Spectre.Console;
using Weasel.Core.Migrations;

namespace Weasel.Core.CommandLine;

[Description("Assert that the existing database(s) matches the current configuration", Name = "db-assert")]
public class AssertCommand: JasperFxAsyncCommand<WeaselInput>
{
    public override async Task<bool> Execute(WeaselInput input)
    {
        AnsiConsole.Write(
            new FigletText("Weasel"){Justification = Justify.Left});

        using var host = input.BuildHost();

        var databases = await input.FilterDatabases(host).ConfigureAwait(false);

        var success = true;
        foreach (var database in databases)
        {
            try
            {
                await database.AssertDatabaseMatchesConfigurationAsync().ConfigureAwait(false);
                AnsiConsole.MarkupLine(
                    $"[green]No database differences detected for '{database.Identifier.EscapeMarkup()}'.[/]");
            }
            catch (DatabaseValidationException e)
            {
                success = false;

                AnsiConsole.MarkupLine(
                    $"[red]Database '{database.Identifier.EscapeMarkup()}' does not match the configuration![/]");
                AnsiConsole.WriteException(e);
            }
        }

        return success;
    }
}
