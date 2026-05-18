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
    ///     hot path. Earlier passes tried <c>[RequiresDynamicCode]</c> on this
    ///     override to propagate the diagnostic; the analyzer rejects that with
    ///     IL3051 because the base member
    ///     (<c>JasperFx.CommandLine.JasperFxAsyncCommand&lt;T&gt;.Execute</c>)
    ///     doesn't carry the same annotation. The
    ///     <see cref="UnconditionalSuppressMessageAttribute" /> below silences the
    ///     underlying IL3050 with a Justification — end users targeting AOT can
    ///     either avoid this command or substitute a non-Spectre exception
    ///     formatter in their own host. Surfaced by Weasel.Core.AotSmoke
    ///     (weasel#263 / JasperFx/jasperfx#213).
    ///     </para>
    /// </summary>
    [UnconditionalSuppressMessage(
        "AOT",
        "IL3050",
        Justification = "AnsiConsole.WriteException's ExceptionFormatter needs runtime IL generation, but this is the dev-time db-assert command path — never reached in an AOT-published consumer. weasel#265.")]
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
