using System.Diagnostics;
using JasperFx.CommandLine;
using JasperFx.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Spectre.Console;
using Weasel.Core.Migrations;

namespace Weasel.Core.CommandLine;

public class WeaselInput: NetCoreInput
{
    private IAnsiConsole? _console;

    [Description("Identify which database to dump in the case of multiple databases. Can be a partial Uri match")]
    public string? DatabaseFlag { get; set; }

    /// <summary>
    ///     Where discovery progress is written. Resolved lazily so that a host which replaces
    ///     <see cref="AnsiConsole.Console" /> after this input is constructed is still respected.
    /// </summary>
    internal IAnsiConsole ProgressConsole
    {
        get => _console ?? AnsiConsole.Console;
        set => _console = value;
    }

    public async ValueTask<List<IDatabase>> AllDatabases(IHost host)
    {
        var databases = host.Services.GetServices<IDatabase>().ToList();
        var sources = host.Services.GetServices<IDatabaseSource>().ToArray();

        // Discovery is not free: a sharded tenancy source resolving 512 databases took an operator
        // 30.6 silent seconds before the apply loop's counter appeared, which from outside is
        // indistinguishable from a hung connection -- runs that were doing fine got killed (weasel#432).
        // So this is announced *before* the work rather than reported after it.
        ProgressConsole.MarkupLine("[gray]Discovering databases...[/]");

        var total = Stopwatch.StartNew();

        foreach (var source in sources)
        {
            var timer = Stopwatch.StartNew();
            var found = await source.BuildDatabases().ConfigureAwait(false);
            timer.Stop();

            databases.AddRange(found);

            // Per source, not just a grand total: when one tenancy source accounts for nearly all of
            // the wait, that points at which implementation to go look at -- which is otherwise
            // invisible from here.
            ProgressConsole.MarkupLine(DescribeSource(source, found.Count, timer.Elapsed));
        }

        total.Stop();

        ProgressConsole.MarkupLine(DescribeTotal(databases.Count, total.Elapsed));

        return databases;
    }

    internal static string DescribeSource(IDatabaseSource source, int count, TimeSpan duration) =>
        $"[gray]  {Markup.Escape(source.GetType().Name)}: {Count(count)} in {Duration(duration)}[/]";

    internal static string DescribeTotal(int count, TimeSpan duration) =>
        $"[gray]Found {Count(count)} in {Duration(duration)}[/]";

    private static string Count(int count) => count == 1 ? "1 database" : $"{count} databases";

    /// <summary>
    ///     Elapsed time an operator can read at a glance. Sub-minute waits are the common case and want
    ///     the tenth of a second; a restore-and-migrate pass runs into the hours, where "5400.0s" is
    ///     arithmetic the reader should not have to do.
    /// </summary>
    private static string Duration(TimeSpan duration)
    {
        if (duration.TotalMinutes < 1)
        {
            return $"{duration.TotalSeconds:0.0}s";
        }

        return duration.TotalHours < 1
            ? $"{duration.Minutes}m {duration.Seconds}s"
            : $"{(int)duration.TotalHours}h {duration.Minutes}m";
    }

    public async ValueTask<IList<IDatabase>> FilterDatabases(IHost host)
    {
        var databases = await AllDatabases(host).ConfigureAwait(false);

        if (!databases.Any())
        {
            throw new InvalidOperationException("No Weasel databases were registered in this application");
        }

        if (DatabaseFlag.IsEmpty()) return databases;

        IList<IDatabase> filtered = [];

        if (DatabaseFlag.IsNotEmpty())
        {
            if (Uri.TryCreate(DatabaseFlag, UriKind.Absolute, out var uri))
            {
                filtered = databases.Where(x =>
                {
                    var descriptor = x.Describe();

                    return descriptor.SubjectUri.Matches(uri) || descriptor.DatabaseUri().Matches(uri);
                }).ToList();
            }
            else
            {
                filtered = databases.Where(x => x.Identifier.EqualsIgnoreCase(DatabaseFlag)).ToList();
            }
        }

        if (!filtered.Any())
        {
            ListCommand.RenderDatabases(databases);
            throw new InvalidOperationException($"No Weasel databases matched the supplied --database flag '{DatabaseFlag}'");
        }

        return filtered;

    }

    public async ValueTask<(bool, IDatabase?)> TryChooseSingleDatabase(IHost host)
    {
        var databases = await AllDatabases(host).ConfigureAwait(false);

        if (!databases.Any())
        {
            AnsiConsole.MarkupLine("[red]No Weasel databases were registered in this application[/]");
            return (false, null);
        }

        if (DatabaseFlag.IsNotEmpty())
        {
            if (Uri.TryCreate(DatabaseFlag, UriKind.Absolute, out var uri))
            {
                var first = databases.FirstOrDefault(x =>
                {
                    var descriptor = x.Describe();

                    return descriptor.SubjectUri.Matches(uri) || descriptor.DatabaseUri().Matches(uri);
                });

                if (first != null)
                {
                    return (true, first);
                }
            }

            var database = databases.FirstOrDefault(x => x.Identifier.EqualsIgnoreCase(DatabaseFlag));
            if (database != null)
            {
                return (true, database);
            }

            AnsiConsole.MarkupLine($"[red]No matching database with either subject or database Uri matching '{DatabaseFlag}'[/].");
            ListCommand.RenderDatabases(databases);

            return (false, null);

        }

        if (databases.Count == 1)
        {
            return (true, databases.Single());
        }

        AnsiConsole.MarkupLine("[bold]A specific database from this list must be selected with the -d|--database flag:[/]");
        ListCommand.RenderDatabases(databases);

        return (false, null);
    }
}
