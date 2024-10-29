using Shouldly;
using Weasel.Core.CommandLine;
using Xunit;

namespace Weasel.CommandLine.Tests;

public class ApplyCommandTests : IntegrationContext
{
    [Fact]
    public async Task easiest_possible_application()
    {
        await DropSchema("one");

        // One database with one feature and one table
        Databases["one"].Features["one"].AddTable("one", "names");

        var success = await ExecuteCommand<ApplyCommand>();
        success.ShouldBeTrue();

        await AssertAllDatabasesMatchConfiguration();
    }

    [Fact]
    public async Task multiple_features()
    {
        await DropSchema("one");

        // One database with one feature and one table
        var database = Databases["one"];
        database.Features["one"].AddTable("one", "names");
        database.Features["one"].AddTable("one", "others");

        var success = await ExecuteCommand<ApplyCommand>();
        success.ShouldBeTrue();

        await AssertAllDatabasesMatchConfiguration();
    }

    [Fact]
    public async Task multiple_databases()
    {
        await DropSchema("one");
        await DropSchema("two");

        // One database with one feature and one table
        var database = Databases["one"];
        database.Features["one"].AddTable("one", "names");
        database.Features["one"].AddTable("one", "others");

        var database2 = Databases["two"];
        database2.Features["two"].AddTable("two", "names2");
        database2.Features["two"].AddTable("two", "others2");

        var success = await ExecuteCommand<ApplyCommand>();
        success.ShouldBeTrue();

        await AssertAllDatabasesMatchConfiguration();
    }
}
