using JasperFx.Core;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Views;
using Xunit;
using Xunit.Abstractions;

namespace Weasel.Postgresql.Tests.Views;

public class ViewTests
{
    private readonly ITestOutputHelper _output;

    public ViewTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void move_to_different_schema()
    {
        var view = new View("myview", "SELECT 1 AS id");
        view.MoveToSchema("other");
        view.Identifier.Schema.ShouldBe("other");
    }

    [Fact]
    public void build_view_by_name_only_puts_it_in_public()
    {
        var view = new View("myview", "SELECT 1 AS id");
        view.Identifier.Schema.ShouldBe("public");
        view.Identifier.Name.ShouldBe("myview");
    }

    [Fact]
    public void smoke_test_writing_view_code()
    {
        var view = new View("people_view", "SELECT 1 AS id");

        var rules = new PostgresqlMigrator();

        var writer = new StringWriter();
        view.WriteCreateStatement(rules, writer);

        var ddl = writer.ToString();

        _output.WriteLine(ddl);

        var lines = ddl.ReadLines().ToArray();

        lines.ShouldContain("DROP VIEW IF EXISTS public.people_view;");
        lines.ShouldContain("CREATE VIEW public.people_view AS SELECT 1 AS id;");
    }
}
