using Shouldly;
using Weasel.Firebird.Views;
using Xunit;

namespace Weasel.Firebird.Tests.Views;

public class ViewTests
{
    private static string[] createStatements(View view)
    {
        var writer = new StringWriter();
        view.WriteCreateStatement(new FirebirdMigrator(), writer);
        return FirebirdScript.Split(writer.ToString()).ToArray();
    }

    [Fact]
    public void the_create_statement_is_one_create_or_alter()
    {
        createStatements(new View("active_users", "select id, name from users where active;"))
            .ShouldBe(["CREATE OR ALTER VIEW active_users AS select id, name from users where active"]);
    }

    [Fact]
    public void a_name_that_must_be_delimited_is_delimited_in_the_folded_spelling()
    {
        createStatements(new View("order view", "select 1 as x from rdb$database")).Single()
            .ShouldStartWith("CREATE OR ALTER VIEW \"ORDER VIEW\" AS");
    }

    [Fact]
    public void the_drop_is_guarded_by_the_view_existing()
    {
        var writer = new StringWriter();
        new View("active_users", "select 1 as x from rdb$database").WriteDropStatement(new FirebirdMigrator(), writer);

        var statement = FirebirdScript.Split(writer.ToString()).Single();
        statement.ShouldContain("RDB$RELATION_NAME = 'ACTIVE_USERS' AND RDB$VIEW_BLR IS NOT NULL");
        statement.ShouldContain("EXECUTE STATEMENT 'DROP VIEW active_users'");
    }

    [Fact]
    public void a_view_in_another_schema_is_refused()
    {
        Should.Throw<NotSupportedException>(() => createStatements(new View("sales.active_users", "select 1 as x from rdb$database")));
    }

    [Fact]
    public void moving_to_another_schema_is_refused_when_the_view_is_written()
    {
        var view = new View("active_users", "select 1 as x from rdb$database");
        view.MoveToSchema("sales");

        Should.Throw<NotSupportedException>(() => createStatements(view));
    }

    [Fact]
    public void basic_create_sql_is_the_create_statement()
    {
        new View("active_users", "select 1 as x from rdb$database").ToBasicCreateViewSql()
            .ShouldBe("CREATE OR ALTER VIEW active_users AS select 1 as x from rdb$database;\n".ReplaceLineEndings());
    }
}
