using JasperFx.CommandLine;
using JasperFx.Core;
using JasperFx.Descriptors;
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
        var completed = 0;
        var unchanged = 0;
        var migrated = 0;

        // AnsiConsole is not safe for concurrent writers -- interleaved MarkupLine calls tear. Every
        // write from inside the batch goes through this lock, and everything belonging to one database
        // (its buffered DDL plus its completion line) is written under a single acquisition so it lands
        // as an attributable unit.
        var console = new object();

        // Only redirect the DDL when appliers actually run concurrently. At --parallel 1 the logger is
        // left alone so a long migration still streams its SQL live -- on a >90 minute restore-and-migrate
        // pass, that stream is what tells the operator the run is alive, and buffering it until completion
        // would trade that away for attribution nothing is threatening.
        var buffer = input.ParallelFlag > 1;

        var failures = await DatabaseBatch.RunAsync(databases, input.ParallelFlag, async (database, token) =>
        {
            var descriptor = database.Describe();

            var ddl = buffer ? redirectMigrationLogger(database) : null;

            try
            {
                // TODO -- it'd be cool to get a rundown of everything that changed.
                var difference = await database.ApplyAllConfiguredChangesWithRetriesAsync(ct: token).ConfigureAwait(false);

                // A 512-database walk with no output until it finishes (or fails) is its own small cruelty --
                // an operator watching this needs to see it moving and be able to estimate completion. Under
                // parallelism a positional (i+1)/total stops meaning anything, so this is a completion
                // counter, incremented as each database finishes.
                var progress = $"({Interlocked.Increment(ref completed)}/{total})";

                switch (difference)
                {
                    case SchemaPatchDifference.None:
                        Interlocked.Increment(ref unchanged);
                        writeCompletion(console, ddl,
                            $"[gray]{progress} No changes detected for DatabaseUri {descriptor.DatabaseUri()} with SubjectUri {descriptor.SubjectUri}.[/]");
                        break;

                    case SchemaPatchDifference.Create:
                    case SchemaPatchDifference.Update:
                        Interlocked.Increment(ref migrated);
                        writeCompletion(console, ddl,
                            $"[bold green]{progress} Successfully applied migrations for DatabaseUri {descriptor.DatabaseUri()} with SubjectUri {descriptor.SubjectUri}.[/]");
                        break;
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e)
            {
                // Reported as it happens so an operator watching a long run sees the failure when it
                // occurs, not minutes later in the summary. The partial DDL still flushes -- what ran
                // before the failing statement is prime diagnostic material.
                var progress = $"({Interlocked.Increment(ref completed)}/{total})";
                writeCompletion(console, ddl,
                    $"[bold red]{progress} Failed to apply migrations for DatabaseUri {descriptor.DatabaseUri()} with SubjectUri {descriptor.SubjectUri}: {Markup.Escape(e.Message)}[/]");

                // The batch runner collects this and keeps going -- one bad shard must not leave the
                // remaining databases unmigrated.
                throw;
            }
            finally
            {
                restoreMigrationLogger(database, ddl);

                // Nothing else needs this database's connections once its apply is done, and this command
                // owns its data sources -- there are no application sessions sharing them. Releasing here
                // keeps peak connection usage at ~the pools actually in flight, instead of trailing an
                // idle pool per database until the connection idle lifetime expires (weasel#356).
                try
                {
                    // Deliberately not the batch token: the pool should be released even when the run is
                    // being cancelled -- that is exactly when abandoning idle connections hurts most.
                    await database.ReleaseConnectionPoolAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    // Releasing the pool is housekeeping. If it throws while an apply is already failing,
                    // letting it out of the finally would discard the migration exception the operator
                    // actually needs to see.
                    lock (console)
                    {
                        AnsiConsole.MarkupLine(
                            $"[yellow]Unable to release the connection pool for DatabaseUri {descriptor.DatabaseUri()}: {Markup.Escape(e.Message)}[/]");
                    }
                }
            }
        }).ConfigureAwait(false);

        writeSummary(total, unchanged, migrated, failures);

        if (failures.Any())
        {
            // Both halves matter: the AggregateException carries every inner failure with its original
            // stack trace, and letting it out of Execute is what turns the process exit code non-zero.
            // Deliberately not a function of the parallelism value -- "fail fast at 1, aggregate at 8"
            // reads as a bug later, so the policy is: always finish the rest, always aggregate.
            throw new AggregateException(
                $"db-apply failed for {failures.Count} of {total} database(s). See the inner exceptions for the per-database failures.",
                failures.Select(x => new DatabaseApplyException(x.Database, x.Exception)));
        }

        return true;
    }

    /// <summary>
    ///     Points a console-bound migration logger at a per-database buffer so several concurrent
    ///     appliers' DDL doesn't interleave line by line on the console. A logger the host already
    ///     redirected somewhere deliberate -- a custom <see cref="IMigrationLogger" />, or a
    ///     <see cref="DefaultMigrationLogger" /> with its own writer -- is left alone.
    /// </summary>
    private static StringWriter? redirectMigrationLogger(IDatabase database)
    {
        if (database is not IDatabaseWithMigrationLogger redirectable) return null;
        if (redirectable.MigrationLogger is not DefaultMigrationLogger { Writer: null }) return null;

        var buffer = new StringWriter();
        redirectable.MigrationLogger = new DefaultMigrationLogger(buffer);
        return buffer;
    }

    private static void restoreMigrationLogger(IDatabase database, StringWriter? ddl)
    {
        if (ddl == null) return;

        // Only ever reached when redirectMigrationLogger swapped a console-bound DefaultMigrationLogger
        // out, so a plain one is the correct restoration.
        ((IDatabaseWithMigrationLogger)database).MigrationLogger = new DefaultMigrationLogger();
    }

    /// <summary>
    ///     Writes one database's completed story as a unit: its buffered DDL (if any) first, then the
    ///     completion line -- the same visual order a sequential run produces, where the SQL streams and
    ///     the line for that database follows it.
    /// </summary>
    private static void writeCompletion(object console, StringWriter? ddl, string markup)
    {
        lock (console)
        {
            var sql = ddl?.ToString();
            if (sql.IsNotEmpty())
            {
                AnsiConsole.Write(sql);
            }

            AnsiConsole.MarkupLine(markup);
        }
    }

    /// <summary>
    ///     The closing rollup. Under parallelism the interleaved per-database lines get genuinely hard
    ///     to read back, and one block of "N unchanged, M migrated, K failed" with the failures listed
    ///     is the cheapest possible answer to "so how did the deploy actually go?".
    /// </summary>
    private static void writeSummary(int total, int unchanged, int migrated, IReadOnlyList<DatabaseFailure> failures)
    {
        var color = failures.Any() ? "bold red" : "bold green";
        AnsiConsole.MarkupLine(
            $"[{color}]Applied {total} database(s): {unchanged} unchanged, {migrated} migrated, {failures.Count} failed.[/]");

        foreach (var failure in failures)
        {
            var descriptor = failure.Database.Describe();
            AnsiConsole.MarkupLine(
                $"[red]  DatabaseUri {descriptor.DatabaseUri()} with SubjectUri {descriptor.SubjectUri}: {Markup.Escape(failure.Exception.Message)}[/]");
        }
    }
}

/// <summary>
///     One database's failure inside a <c>db-apply</c> run, wrapped so the database's identity travels
///     with the original exception (and its stack trace) inside the terminal
///     <see cref="AggregateException" />.
/// </summary>
public class DatabaseApplyException: Exception
{
    internal DatabaseApplyException(IDatabase database, Exception inner)
        : base(describe(database), inner)
    {
    }

    private static string describe(IDatabase database)
    {
        var descriptor = database.Describe();
        return $"Failed to apply migrations for DatabaseUri {descriptor.DatabaseUri()} with SubjectUri {descriptor.SubjectUri}";
    }
}
