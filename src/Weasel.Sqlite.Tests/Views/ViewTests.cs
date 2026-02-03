using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Views;
using Xunit;

namespace Weasel.Sqlite.Tests.Views;

public class ViewTests
{
    [Fact]
    public void create_view_with_simple_name()
    {
        var view = new View("active_users", "SELECT * FROM users WHERE active = 1");

        view.Identifier.Name.ShouldBe("active_users");
        view.Identifier.Schema.ShouldBe("main");
        view.ViewSql.ShouldBe("SELECT * FROM users WHERE active = 1");
    }

    [Fact]
    public void create_view_with_schema_qualified_name()
    {
        var view = new View("temp.active_users", "SELECT * FROM users WHERE active = 1");

        view.Identifier.Name.ShouldBe("active_users");
        view.Identifier.Schema.ShouldBe("temp");
    }

    [Fact]
    public void generate_create_view_statement()
    {
        var view = new View("active_users", "SELECT id, name FROM users WHERE active = 1");

        var sql = view.ToBasicCreateViewSql();

        sql.ShouldContain("DROP VIEW IF EXISTS");
        sql.ShouldContain("main.active_users");
        sql.ShouldContain("CREATE VIEW");
        sql.ShouldContain("SELECT id, name FROM users WHERE active = 1");
    }

    [Fact]
    public void generate_create_view_statement_adds_semicolon_if_missing()
    {
        var view = new View("active_users", "SELECT * FROM users");

        var sql = view.ToBasicCreateViewSql();

        sql.ShouldContain("SELECT * FROM users;");
    }

    [Fact]
    public void generate_create_view_statement_preserves_existing_semicolon()
    {
        var view = new View("active_users", "SELECT * FROM users;");

        var sql = view.ToBasicCreateViewSql();

        // Should not have double semicolons
        sql.ShouldNotContain(";;");
        sql.ShouldContain("SELECT * FROM users;");
    }

    [Fact]
    public void generate_drop_view_statement()
    {
        var view = new View("active_users", "SELECT * FROM users WHERE active = 1");

        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        view.WriteDropStatement(migrator, writer);
        var sql = writer.ToString();

        sql.ShouldContain("DROP VIEW IF EXISTS");
        sql.ShouldContain("main.active_users");
    }

    [Fact]
    public void view_with_join()
    {
        var viewSql = @"
            SELECT u.id, u.name, o.order_count
            FROM users u
            LEFT JOIN (
                SELECT user_id, COUNT(*) as order_count
                FROM orders
                GROUP BY user_id
            ) o ON u.id = o.user_id";

        var view = new View("user_order_summary", viewSql);

        var sql = view.ToBasicCreateViewSql();

        sql.ShouldContain("CREATE VIEW");
        sql.ShouldContain("user_order_summary");
        sql.ShouldContain("LEFT JOIN");
    }

    [Fact]
    public void view_with_where_clause()
    {
        var view = new View("recent_orders",
            "SELECT * FROM orders WHERE created_at > date('now', '-30 days')");

        var sql = view.ToBasicCreateViewSql();

        sql.ShouldContain("WHERE created_at > date('now', '-30 days')");
    }

    [Fact]
    public void view_with_aggregation()
    {
        var view = new View("daily_sales",
            @"SELECT DATE(created_at) as sale_date, SUM(amount) as total
              FROM orders
              GROUP BY DATE(created_at)");

        var sql = view.ToBasicCreateViewSql();

        sql.ShouldContain("SUM(amount)");
        sql.ShouldContain("GROUP BY");
    }

    [Fact]
    public void all_names_returns_identifier()
    {
        var view = new View("test_view", "SELECT 1");

        var names = view.AllNames().ToList();

        names.Count.ShouldBe(1);
        names[0].Name.ShouldBe("test_view");
    }

    [Fact]
    public void throws_when_view_sql_is_null()
    {
        Should.Throw<ArgumentNullException>(() => new View("test", (string)null!));
    }
}
