using Shouldly;
using Weasel.Firebird.Functions;
using Xunit;

namespace Weasel.Firebird.Tests.Functions;

public class FunctionTests
{
    private static string[] createStatements(Function function)
    {
        var writer = new StringWriter();
        function.WriteCreateStatement(new FirebirdMigrator(), writer);
        return FirebirdScript.Split(writer.ToString()).ToArray();
    }

    [Fact]
    public void the_create_statement_is_one_psql_create_or_alter()
    {
        var function = new Function("fn_double", "CREATE FUNCTION fn_double (n INTEGER) RETURNS INTEGER AS BEGIN RETURN n * 2; END;");

        var writer = new StringWriter();
        function.WriteCreateStatement(new FirebirdMigrator(), writer);

        writer.ToString().ShouldStartWith("SET TERM ^ ;");
        FirebirdScript.Split(writer.ToString())
            .ShouldBe(["CREATE OR ALTER FUNCTION fn_double (n INTEGER) RETURNS INTEGER AS BEGIN RETURN n * 2; END"]);
    }

    /// <summary>
    ///     A delimited name keeps its case and an undelimited one is folded, so a statement that spells
    ///     the name differently from the identifier creates a function introspection never finds -- a
    ///     <c>Create</c> reported forever. It is refused instead.
    /// </summary>
    [Theory]
    [InlineData("fn_double", "CREATE FUNCTION \"fn_double\" RETURNS INTEGER AS BEGIN RETURN 1; END")]
    [InlineData("fn_double", "CREATE FUNCTION fn_triple RETURNS INTEGER AS BEGIN RETURN 1; END")]
    public void a_statement_creating_another_function_is_refused(string name, string statement)
    {
        var ex = Should.Throw<ArgumentException>(() => createStatements(new Function(name, statement)));
        ex.Message.ShouldContain("FN_DOUBLE");
    }

    [Fact]
    public void the_name_may_be_spelled_in_any_case_when_undelimited()
    {
        createStatements(new Function("FN_Double", "create function fn_double returns integer as begin return 1; end"))
            .Single().ShouldStartWith("CREATE OR ALTER FUNCTION fn_double");
    }

    /// <summary>
    ///     weasel#624: a caret outside a literal would end the statement early in isql.
    /// </summary>
    [Fact]
    public void a_caret_in_the_body_is_refused()
    {
        Should.Throw<ArgumentException>(() => createStatements(new Function("fn_flag",
            "CREATE FUNCTION fn_flag (n INTEGER) RETURNS BOOLEAN AS BEGIN RETURN n ^= 0; END")));
    }

    [Fact]
    public void the_drop_is_guarded_by_the_function_existing()
    {
        var writer = new StringWriter();
        new Function("fn_double", "CREATE FUNCTION fn_double RETURNS INTEGER AS BEGIN RETURN 1; END")
            .WriteDropStatement(new FirebirdMigrator(), writer);

        var statement = FirebirdScript.Split(writer.ToString()).Single();
        statement.ShouldContain("RDB$FUNCTION_NAME = 'FN_DOUBLE' AND RDB$PACKAGE_NAME IS NULL");
        statement.ShouldContain("EXECUTE STATEMENT 'DROP FUNCTION fn_double'");
    }

    [Fact]
    public void custom_drop_statements_are_written_one_per_statement()
    {
        var writer = new StringWriter();
        new Function(new FirebirdObjectName("fn_double"), "CREATE FUNCTION fn_double RETURNS INTEGER AS BEGIN RETURN 1; END",
                ["DROP FUNCTION fn_double", "DROP FUNCTION fn_double_old"])
            .WriteDropStatement(new FirebirdMigrator(), writer);

        FirebirdScript.Split(writer.ToString()).ShouldBe(["DROP FUNCTION fn_double", "DROP FUNCTION fn_double_old"]);
    }

    [Fact]
    public void a_function_marked_for_removal_creates_nothing()
    {
        createStatements(Function.ForRemoval("fn_double")).ShouldBeEmpty();
    }

    [Fact]
    public void a_function_in_another_schema_is_refused()
    {
        Should.Throw<NotSupportedException>(() => createStatements(new Function("sales.fn_double",
            "CREATE FUNCTION fn_double RETURNS INTEGER AS BEGIN RETURN 1; END")));
    }
}
