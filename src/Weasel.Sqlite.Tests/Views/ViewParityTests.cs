using Shouldly;
using Weasel.Sqlite.Views;
using Xunit;

namespace Weasel.Sqlite.Tests.Views;

/// <summary>
/// Tests to verify View API parity with PostgreSQL
/// </summary>
public class ViewParityTests
{
    [Fact]
    public void view_move_to_schema()
    {
        var view = new View("user_summary", "SELECT id, name FROM users");

        view.Identifier.Schema.ShouldBe("main");
        view.Identifier.Name.ShouldBe("user_summary");

        view.MoveToSchema("temp");

        view.Identifier.Schema.ShouldBe("temp");
        view.Identifier.Name.ShouldBe("user_summary");
    }

    [Fact]
    public void view_move_to_schema_preserves_sql()
    {
        var sql = "SELECT id, name, email FROM users WHERE active = 1";
        var view = new View("active_users", sql);

        view.MoveToSchema("temp");

        view.ViewSql.ShouldBe(sql);
    }

    [Fact]
    public void view_move_to_schema_updates_qualified_name()
    {
        var view = new View("user_summary", "SELECT * FROM users");

        view.MoveToSchema("temp");

        view.Identifier.QualifiedName.ShouldBe("\"temp\".user_summary");
    }
}
