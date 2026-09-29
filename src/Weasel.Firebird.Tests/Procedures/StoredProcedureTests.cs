using Shouldly;
using Weasel.Firebird.Procedures;
using Xunit;

namespace Weasel.Firebird.Tests.Procedures;

public class StoredProcedureTests
{
    private static string[] createStatements(StoredProcedure procedure)
    {
        var writer = new StringWriter();
        procedure.WriteCreateStatement(new FirebirdMigrator(), writer);
        return FirebirdScript.Split(writer.ToString()).ToArray();
    }

    [Fact]
    public void the_create_statement_is_one_psql_create_or_alter()
    {
        createStatements(new StoredProcedure("sp_touch", "RECREATE PROCEDURE sp_touch AS BEGIN INSERT INTO t (id) VALUES (1); END"))
            .ShouldBe(["CREATE OR ALTER PROCEDURE sp_touch AS BEGIN INSERT INTO t (id) VALUES (1); END"]);
    }

    [Fact]
    public void a_statement_creating_another_procedure_is_refused()
    {
        Should.Throw<ArgumentException>(() =>
            createStatements(new StoredProcedure("sp_touch", "CREATE PROCEDURE sp_other AS BEGIN END")));
    }

    [Fact]
    public void a_statement_that_is_a_function_is_refused()
    {
        Should.Throw<ArgumentException>(() => createStatements(new StoredProcedure("sp_touch",
            "CREATE FUNCTION sp_touch RETURNS INTEGER AS BEGIN RETURN 1; END")));
    }

    [Fact]
    public void the_drop_is_guarded_by_the_procedure_existing()
    {
        var writer = new StringWriter();
        new StoredProcedure("sp_touch", "CREATE PROCEDURE sp_touch AS BEGIN END").WriteDropStatement(new FirebirdMigrator(), writer);

        var statement = FirebirdScript.Split(writer.ToString()).Single();
        statement.ShouldContain("RDB$PROCEDURE_NAME = 'SP_TOUCH' AND RDB$PACKAGE_NAME IS NULL");
        statement.ShouldContain("EXECUTE STATEMENT 'DROP PROCEDURE sp_touch'");
    }

    [Fact]
    public void a_removed_procedure_creates_nothing()
    {
        createStatements(new StoredProcedure("sp_touch", "CREATE PROCEDURE sp_touch AS BEGIN END") { IsRemoved = true })
            .ShouldBeEmpty();
    }
}
