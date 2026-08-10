using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Shouldly;
using Spectre.Console;
using Weasel.Core.CommandLine;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.CommandLine.Tests;

/// <summary>
///     Coverage for weasel#432: database discovery ran in silence. At 512 shard databases that was
///     30.6 seconds with no output before the apply loop's counter appeared, which an operator cannot
///     tell apart from a hung connection -- runs that were doing fine got killed.
/// </summary>
public class DatabaseDiscoveryProgressTests
{
    private readonly StringWriter theOutput = new();
    private readonly WeaselInput theInput = new();

    public DatabaseDiscoveryProgressTests()
    {
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Out = new AnsiConsoleOutput(theOutput)
        });

        // Otherwise Spectre wraps at the default width and splits the lines being asserted on.
        console.Profile.Width = 500;

        theInput.ProgressConsole = console;
    }

    private IHost BuildHost(params IDatabaseSource[] sources)
    {
        var builder = new HostBuilder();
        builder.ConfigureServices(services =>
        {
            foreach (var source in sources)
            {
                services.AddSingleton(source);
            }
        });

        theInput.HostBuilder = builder;
        return theInput.BuildHost();
    }

    private static IDatabaseSource SourceOf(int count, Func<IReadOnlyList<IDatabase>>? onBuild = null)
    {
        var databases = Enumerable.Range(1, count).Select(_ => Substitute.For<IDatabase>()).ToArray();

        var source = Substitute.For<IDatabaseSource>();
        source.BuildDatabases().Returns(_ => onBuild?.Invoke() ?? databases);

        return source;
    }

    private string[] Lines() => theOutput.ToString()
        .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries)
        .Select(x => x.TrimEnd())
        .ToArray();

    [Fact]
    public async Task announces_discovery_before_any_source_is_built()
    {
        // The whole point of the issue: the announcement has to precede the wait, not describe it
        // afterwards. So look at what has been written at the moment a source is asked to build.
        var outputWhenCalled = string.Empty;
        var source = SourceOf(2, () =>
        {
            outputWhenCalled = theOutput.ToString();
            return [Substitute.For<IDatabase>(), Substitute.For<IDatabase>()];
        });

        using var host = BuildHost(source);
        await theInput.AllDatabases(host);

        outputWhenCalled.ShouldContain("Discovering databases...");
    }

    [Fact]
    public async Task reports_each_source_and_then_the_total()
    {
        using var host = BuildHost(SourceOf(3), SourceOf(2));

        var databases = await theInput.AllDatabases(host);
        databases.Count.ShouldBe(5);

        var lines = Lines();
        lines[0].ShouldBe("Discovering databases...");
        lines[1].ShouldContain(": 3 databases in ");
        lines[2].ShouldContain(": 2 databases in ");
        lines[3].ShouldStartWith("Found 5 databases in ");
    }

    [Fact]
    public async Task reports_a_run_with_no_sources_at_all()
    {
        using var host = BuildHost();
        await theInput.AllDatabases(host);

        Lines().Last().ShouldStartWith("Found 0 databases in ");
    }

    [Theory]
    [InlineData(1, "1 database")]
    [InlineData(0, "0 databases")]
    [InlineData(1037, "1037 databases")]
    public void counts_are_pluralized(int count, string expected)
    {
        WeaselInput.DescribeTotal(count, TimeSpan.Zero)
            .ShouldBe($"[gray]Found {expected} in 0.0s[/]");
    }

    [Theory]
    [InlineData(0.04, "0.0s")]
    [InlineData(30.64, "30.6s")]
    [InlineData(59.9, "59.9s")]
    [InlineData(60, "1m 0s")]
    [InlineData(312, "5m 12s")]
    [InlineData(5400, "1h 30m")]
    public void durations_stay_readable_at_every_scale(double seconds, string expected)
    {
        WeaselInput.DescribeTotal(1, TimeSpan.FromSeconds(seconds))
            .ShouldBe($"[gray]Found 1 database in {expected}[/]");
    }
}
