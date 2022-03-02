using Baseline;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Oakton;
using Shouldly;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.CommandLine.Tests;

public class DumpCommandTests : IntegrationContext
{
    internal Task<bool> ExecuteDumpCommand(DumpInput input)
    {
        var command = new DumpCommand();
        var builder = Host.CreateDefaultBuilder().ConfigureServices(services =>
        {
            foreach (var database in Databases.GetAll())
            {
                services.AddSingleton<IDatabase>(database);
            }
        });

        input.HostBuilder = builder;
        return command.Execute(input);
    }

    [Fact]
    public async Task run_repeatedly_for_writing_by_feature_directory()
    {
        // One database with one feature and one table
        var database = Databases["one"];
        database.Features["one"].AddTable("one", "names");
        database.Features["one"].AddTable("one", "others");

        var input = new DumpInput
        {
            Path = Path.GetTempPath().AppendPath("dump1"),
            ByFeatureFlag = true
        };

        (await ExecuteDumpCommand(input)).ShouldBeTrue();
        await Task.Delay(100); // Let the file system calm down

        (await ExecuteDumpCommand(input)).ShouldBeTrue();

        Directory.Exists(input.Path).ShouldBeTrue();

        var files = Directory.EnumerateFiles(input.Path)
            .Select(Path.GetFileName)
            .OrderBy(x => x)
            .ToArray();

        files.ShouldBe(new string[]{"all.sql", "one.sql", "schemas.sql"});

    }

    [Fact]
    public async Task run_repeatedly_for_file()
    {
        // One database with one feature and one table
        var database = Databases["one"];
        database.Features["one"].AddTable("one", "names");
        database.Features["one"].AddTable("one", "others");


        var input = new DumpInput
        {
            Path = Path.GetTempPath().AppendPath("dump2", "file.sql"),
        };


        (await ExecuteDumpCommand(input)).ShouldBeTrue();
        await Task.Delay(100); // Let the file system calm down

        (await ExecuteDumpCommand(input)).ShouldBeTrue();

        File.Exists(input.Path).ShouldBeTrue();
    }
}

