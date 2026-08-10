using System.Diagnostics.CodeAnalysis;
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

        // AnsiConsole is not safe for concurrent writers, so every write from inside the batch takes
        // this lock and a database's failure story (headline plus exception) lands as one unit.
        var console = new object();

        // db-assert has the identical many-database loop -- and the identical sequential pain at fleet
        // scale -- as db-apply, which is why the parallelism knob lives on WeaselInput and the shared
        // batch runner is used here too (weasel#431). Assertion failures were already collected rather
        // than fatal; the runner preserves exactly that: every database is checked, the command fails
        // if any database failed.
        var failures = await DatabaseBatch.RunAsync(databases, input.ParallelFlag, async (database, token) =>
        {
            try
            {
                await database.AssertDatabaseMatchesConfigurationAsync(token).ConfigureAwait(false);
                lock (console)
                {
                    AnsiConsole.MarkupLine(
                        $"[green]No database differences detected for '{Markup.Escape(database.Identifier)}'.[/]");
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e)
            {
                writeFailure(console, database, e);
                throw;
            }
        }).ConfigureAwait(false);

        return !failures.Any();
    }

    /// <summary>
    ///     Uses <see cref="AnsiConsole.WriteException(Exception, ExceptionFormats)" /> to
    ///     pretty-print database validation failures. Spectre.Console's exception
    ///     formatter uses runtime IL generation that isn't available under
    ///     <c>PublishAot</c>, which surfaced as IL3050 with <c>IsAotCompatible=true</c>
    ///     on Weasel.Core.
    ///     <para>
    ///     This is a dev-time CLI tool (the <c>db-assert</c> command), not on any
    ///     hot path. Earlier passes tried <c>[RequiresDynamicCode]</c> to propagate the
    ///     diagnostic; the analyzer rejected that with IL3051 when the annotation sat on
    ///     the <c>Execute</c> override, because the base member
    ///     (<c>JasperFx.CommandLine.JasperFxAsyncCommand&lt;T&gt;.Execute</c>)
    ///     doesn't carry the same annotation. The
    ///     <see cref="UnconditionalSuppressMessageAttribute" /> below silences the
    ///     underlying IL3050 with a Justification — end users targeting AOT can
    ///     either avoid this command or substitute a non-Spectre exception
    ///     formatter in their own host. Surfaced by Weasel.Core.AotSmoke
    ///     (weasel#263 / JasperFx/jasperfx#213). Lives on this helper rather than on
    ///     <c>Execute</c> because the call moved into a lambda for weasel#431's parallel
    ///     batch, and the suppression does not flow into a lambda's generated method.
    ///     </para>
    /// </summary>
    [UnconditionalSuppressMessage(
        "AOT",
        "IL3050",
        Justification = "AnsiConsole.WriteException's ExceptionFormatter needs runtime IL generation, but this is the dev-time db-assert command path — never reached in an AOT-published consumer. weasel#265.")]
    private static void writeFailure(object console, IDatabase database, Exception exception)
    {
        lock (console)
        {
            AnsiConsole.MarkupLine(
                exception is DatabaseValidationException
                    ? $"[red]Database '{Markup.Escape(database.Identifier)}' does not match the configuration![/]"
                    : $"[red]Failed to assert database '{Markup.Escape(database.Identifier)}'![/]");
            AnsiConsole.WriteException(exception);
        }
    }
}
