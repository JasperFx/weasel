using Oakton;
using Spectre.Console;
using Weasel.Core.Migrations;

namespace Weasel.CommandLine;

[Description("Assert that the existing database(s) matches the current configuration", Name = "db-assert")]
public class AssertCommand: OaktonAsyncCommand<WeaselInput>
{
    public override async Task<bool> Execute(WeaselInput input)
    {
        using var host = input.BuildHost();

        var databases = await input.FilterDatabases(host);

        var success = true;
        foreach (var database in databases)
        {
            try
            {
                await database.AssertDatabaseMatchesConfigurationAsync();
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
