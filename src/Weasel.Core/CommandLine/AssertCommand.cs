using System.Diagnostics.CodeAnalysis;
using JasperFx.CommandLine;
using Spectre.Console;
using Weasel.Core.Migrations;

namespace Weasel.Core.CommandLine;

[Description("Assert that the existing database(s) matches the current configuration", Name = "db-assert")]
public class AssertCommand: JasperFxAsyncCommand<WeaselInput>
{
    /// <summary>
    ///     Uses <see cref="AnsiConsole.WriteException(Exception, ExceptionFormats)" /> to
    ///     pretty-print database validation failures. Spectre.Console's exception
    ///     formatter uses runtime IL generation that isn't available under
    ///     <c>PublishAot</c>, which surfaced as IL3050 with <c>IsAotCompatible=true</c>
    ///     on Weasel.Core.
    ///     <para>
    ///     This is a dev-time CLI tool (the <c>db-assert</c> command), not on any
    ///     hot path — annotating the entry point so the warning propagates to AOT-
    ///     publishing consumers as a precise diagnostic is the right minimum-blast-
    ///     radius fix (per JasperFx/jasperfx#213). End users targeting AOT can
    ///     either avoid this command, or substitute a non-Spectre exception
    ///     formatter in their own host.
    ///     </para>
    /// </summary>
    [RequiresDynamicCode("Uses Spectre.Console.AnsiConsole.WriteException, whose ExceptionFormatter requires runtime IL generation that isn't available under PublishAot.")]
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
