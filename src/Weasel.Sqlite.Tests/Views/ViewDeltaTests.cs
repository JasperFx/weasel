using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Views;
using Xunit;

namespace Weasel.Sqlite.Tests.Views;

public class ViewDeltaTests
{
    [Fact]
    public void no_difference_when_views_match()
    {
        var expected = new View("test_view", "SELECT id, name FROM users");
        var actual = new View("test_view", "SELECT id, name FROM users");

        var delta = new ViewDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void no_difference_ignores_whitespace_differences()
    {
        var expected = new View("test_view", "SELECT id, name FROM users");
        var actual = new View("test_view", "SELECT id,name FROM users"); // No space after comma

        var delta = new ViewDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void no_difference_ignores_line_breaks()
    {
        var expected = new View("test_view", "SELECT id, name FROM users");
        var actual = new View("test_view", @"SELECT id, name
FROM users");

        var delta = new ViewDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void no_difference_ignores_trailing_semicolon()
    {
        var expected = new View("test_view", "SELECT * FROM users");
        var actual = new View("test_view", "SELECT * FROM users;");

        var delta = new ViewDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void create_when_view_does_not_exist()
    {
        var expected = new View("test_view", "SELECT * FROM users");

        var delta = new ViewDelta(expected, null);

        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public void update_when_view_sql_changed()
    {
        var expected = new View("test_view", "SELECT id, name, email FROM users");
        var actual = new View("test_view", "SELECT id, name FROM users");

        var delta = new ViewDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public void write_update_creates_view_when_missing()
    {
        var expected = new View("test_view", "SELECT * FROM users");
        var delta = new ViewDelta(expected, null);

        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        delta.WriteUpdate(migrator, writer);

        var sql = writer.ToString();

        sql.ShouldContain("DROP VIEW IF EXISTS");
        sql.ShouldContain("CREATE VIEW");
        sql.ShouldContain("test_view");
    }

    [Fact]
    public void write_update_recreates_view_when_changed()
    {
        var expected = new View("test_view", "SELECT id, name, email FROM users");
        var actual = new View("test_view", "SELECT id, name FROM users");

        var delta = new ViewDelta(expected, actual);

        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        delta.WriteUpdate(migrator, writer);

        var sql = writer.ToString();

        sql.ShouldContain("DROP VIEW IF EXISTS");
        sql.ShouldContain("CREATE VIEW");
        sql.ShouldContain("email"); // Should have the new column
    }

    [Fact]
    public void write_update_does_nothing_when_no_difference()
    {
        var expected = new View("test_view", "SELECT * FROM users");
        var actual = new View("test_view", "SELECT * FROM users");

        var delta = new ViewDelta(expected, actual);

        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        delta.WriteUpdate(migrator, writer);

        var sql = writer.ToString();

        sql.ShouldBeEmpty();
    }

    [Fact]
    public void write_rollback_restores_previous_view()
    {
        var expected = new View("test_view", "SELECT id, name, email FROM users");
        var actual = new View("test_view", "SELECT id, name FROM users");

        var delta = new ViewDelta(expected, actual);

        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        delta.WriteRollback(migrator, writer);

        var sql = writer.ToString();

        sql.ShouldContain("CREATE VIEW");
        sql.ShouldNotContain("email"); // Should restore the old definition without email
    }

    [Fact]
    public void write_rollback_drops_view_when_it_didnt_exist()
    {
        var expected = new View("test_view", "SELECT * FROM users");
        var delta = new ViewDelta(expected, null);

        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        delta.WriteRollback(migrator, writer);

        var sql = writer.ToString();

        sql.ShouldContain("DROP VIEW IF EXISTS");
        sql.ShouldNotContain("CREATE VIEW");
    }

    [Fact]
    public void difference_is_case_insensitive()
    {
        var expected = new View("test_view", "SELECT id FROM users");
        var actual = new View("test_view", "select ID from USERS");

        var delta = new ViewDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
