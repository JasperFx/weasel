using JasperFx;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Shouldly;
using Weasel.Core;
using Weasel.Core.CommandLine;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.CommandLine.Tests;

public class WeaselInputTests
{
    private readonly IDatabase database1;
    private readonly IDatabase database2;
    private readonly IDatabase database3;
    private readonly IDatabase database4;
    private readonly IDatabase database5;
    private readonly IDatabase database6;
    private readonly IDatabase database7;
    private readonly IDatabaseSource source1;
    private readonly IDatabaseSource source2;

    private readonly WeaselInput theInput = new WeaselInput();

    public WeaselInputTests()
    {
        var builder = new HostBuilder();

        database1 = new DatabaseWithTables(AutoCreate.All, "One");
        database2 = new DatabaseWithTables(AutoCreate.All, "Two");
        database3 = new DatabaseWithTables(AutoCreate.All, "Three");

        source1 = Substitute.For<IDatabaseSource>();
        database4 = new DatabaseWithTables(AutoCreate.All, "Four");
        database5 = new DatabaseWithTables(AutoCreate.All, "Five");

        source1.BuildDatabases().Returns(new[] { database4, database5 });

        source2 = Substitute.For<IDatabaseSource>();
        database6 = new DatabaseWithTables(AutoCreate.All, "Six");
        database7 = new DatabaseWithTables(AutoCreate.All, "Seven");

        source2.BuildDatabases().Returns(new[] { database6, database7 });

        builder.ConfigureServices(services =>
        {
            services.AddSingleton<IDatabase>(database1);
            services.AddSingleton<IDatabase>(database2);
            services.AddSingleton<IDatabase>(database3);

            services.AddSingleton(source1);
            services.AddSingleton(source2);
        });

        theInput.HostBuilder = builder;
    }

    [Fact]
    public async Task filter_all_databases_with_no_selections()
    {
        using var host = theInput.BuildHost();
        var databases = await theInput.FilterDatabases(host);
        databases
            .ShouldBe(new[]{database1, database2, database3, database4, database5, database6, database7});

    }

    [Fact]
    public async Task find_one_from_source()
    {
        theInput.DatabaseFlag = "Seven";
        using var host = theInput.BuildHost();
        var (found, database) = await theInput.TryChooseSingleDatabase(host);
        found.ShouldBeTrue();

        database.Identifier.ShouldBe("Seven");

    }

    [Fact]
    public async Task filter_by_database_flag()
    {
        theInput.DatabaseFlag = database2.Identifier;
        using var host = theInput.BuildHost();
        (await theInput.FilterDatabases(host))
            .Single().ShouldBe(database2);
    }

    [Fact]
    public async Task throw_if_no_databases()
    {
        await Should.ThrowAsync<InvalidOperationException>(async () =>
        {
            using var host = theInput.BuildHost();
            theInput.DatabaseFlag = Guid.NewGuid().ToString();
            (await theInput.FilterDatabases(host)).ShouldBeEmpty();
        });
    }

    [Fact]
    public async Task interactive_filtering()
    {
        var input = Substitute.For<WeaselInput>();
        input.SelectOptions(Arg.Any<List<IDatabase>>())
            .Returns(new List<string> { database3.Identifier, database7.Identifier });

        input.InteractiveFlag = true;

        using var host = theInput.BuildHost();
        (await input.FilterDatabases(host))
            .ShouldBe(new[]{database3, database7});
    }
}
