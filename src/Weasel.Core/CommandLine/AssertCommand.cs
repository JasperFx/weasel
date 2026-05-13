using System.Diagnostics.CodeAnalysis;
using JasperFx.CommandLine;
using Spectre.Console;
using Weasel.Core.Migrations;

namespace Weasel.Core.CommandLine;

[Description("Assert that the existing database(s) matches the current configuration", Name = "db-assert")]
public class AssertCommand: JasperFxAsyncCommand<WeaselInput>
{
    // weasel#265: Spectre.Console.AnsiConsole.WriteException calls into
    // an ExceptionFormatter path that's RequiresDynamicCode. db-assert is
    // a dev-time CLI tool (not a production hot path), so suppressing the
    // warning here is the right call rather than propagating it via
    // [RequiresDynamicCode] — propagation triggers IL3051 because the
    // JasperFx base method JasperFxAsyncCommand<T>.Execute(T) doesn't
    // carry the attribute, and adding it there would ripple to every
    // JasperFx command across the Critter Stack. Suppression keeps the
    // fix Weasel-scoped. If JasperFx ever annotates the base method,
    // delete this suppression and use [RequiresDynamicCode] instead.
    [UnconditionalSuppressMessage(
        "AOT",
        "IL3050",
        Justification = "AnsiConsole.WriteException is only reached on the dev-time db-assert command path. weasel#265 / JasperFx/jasperfx#213.")]
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
                    $"[green]No database differences detected for '{Markup.Escape(database.Identifier)}'.[/]");
            }
            catch (DatabaseValidationException e)
            {
                success = false;

                AnsiConsole.MarkupLine(
                    $"[red]Database '{Markup.Escape(database.Identifier)}' does not match the configuration![/]");
                AnsiConsole.WriteException(e);
            }
        }

        return success;
    }
}
