using Shouldly;
using Weasel.Core.CommandLine;
using Xunit;

namespace Weasel.CommandLine.Tests;

public class AssertCommandTests : IntegrationContext
{
    [Fact]
    public async Task simple_happy_path()
    {
        await DropSchema("one");

        // One database with one feature and one table
        Databases["one"].Features["one"].AddTable("one", "names");

        await ExecuteCommand<ApplyCommand>();

        (await ExecuteCommand<AssertCommand>()).ShouldBeTrue();
    }

    [Fact]
    public async Task simple_sad_path()
    {
        await DropSchema("one");

        // One database with one feature and one table
        Databases["one"].Features["one"].AddTable("one", "names");

        await ExecuteCommand<ApplyCommand>();

        Databases["one"].Features["one"].AddTable("one", "others");

        (await ExecuteCommand<AssertCommand>()).ShouldBeFalse();
    }

    [Fact]
    public async Task happy_path_multiple_features()
    {
        await DropSchema("one");

        // One database with one feature and one table
        var database = Databases["one"];
        database.Features["one"].AddTable("one", "names");
        database.Features["one"].AddTable("one", "others");

        await ExecuteCommand<ApplyCommand>();

        (await ExecuteCommand<AssertCommand>()).ShouldBeTrue();
    }

    [Fact]
    public async Task sad_path_multiple_features()
    {
        await DropSchema("one");

        // One database with one feature and one table
        var database = Databases["one"];
        database.Features["one"].AddTable("one", "names");
        database.Features["one"].AddTable("one", "others");

        await ExecuteCommand<ApplyCommand>();

        database.Features["one"].AddTable("one", "additional");

        (await ExecuteCommand<AssertCommand>()).ShouldBeFalse();
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

        await ExecuteCommand<ApplyCommand>();
        (await ExecuteCommand<AssertCommand>()).ShouldBeTrue();
    }

    [Fact]
    public async Task multiple_databases_sad_path()
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

        await ExecuteCommand<ApplyCommand>();

        database2.Features["two"].AddTable("two", "additional2");

        (await ExecuteCommand<AssertCommand>()).ShouldBeFalse();
    }
}
