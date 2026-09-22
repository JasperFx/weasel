using Shouldly;
using Weasel.SqlServer.Procedures;
using Xunit;

namespace Weasel.SqlServer.Tests.Procedures;

/// <summary>
///     A rendered migration script concatenates every object's DDL into one file, and
///     <c>CREATE OR ALTER PROCEDURE</c> has to be the first statement of its own batch. So both
///     create paths emit the same thing: the statement bracketed by <c>GO</c> lines, with the
///     preamble normalized to the idempotent form (weasel#593).
/// </summary>
public class stored_procedure_ddl
{
    private static readonly SqlServerObjectName theIdentifier = new("dbo", "p");

    private const string OneLineBody = "CREATE PROCEDURE dbo.p AS SELECT 1;";

    private static string writeCreate(StoredProcedure procedure)
    {
        var writer = new StringWriter();
        procedure.WriteCreateStatement(new SqlServerMigrator(), writer);
        return writer.ToString();
    }

    private static string writeCreateOrAlter(StoredProcedure procedure)
    {
        var writer = new StringWriter();
        procedure.WriteCreateOrAlterStatement(new SqlServerMigrator(), writer);
        return writer.ToString();
    }

    private static string batched(string body)
        => $"GO{Environment.NewLine}{body}{Environment.NewLine}GO{Environment.NewLine}";

    [Fact]
    public void the_create_statement_is_the_normalized_body_between_go_lines()
    {
        writeCreate(new StoredProcedure(theIdentifier, OneLineBody))
            .ShouldBe(batched("CREATE OR ALTER PROCEDURE dbo.p AS SELECT 1;"));
    }

    [Fact]
    public void the_create_or_alter_statement_is_the_same_text()
    {
        var procedure = new StoredProcedure(theIdentifier, OneLineBody);

        writeCreateOrAlter(procedure).ShouldBe(writeCreate(procedure));
    }

    [Theory]
    [InlineData("CREATE PROCEDURE dbo.p AS SELECT 1;")]
    [InlineData("CREATE PROC dbo.p AS SELECT 1;")]
    [InlineData("create procedure dbo.p AS SELECT 1;")]
    [InlineData("Create Or Alter Proc dbo.p AS SELECT 1;")]
    [InlineData("CREATE OR ALTER PROCEDURE dbo.p AS SELECT 1;")]
    public void every_spelling_of_the_preamble_lands_on_create_or_alter_procedure(string body)
    {
        // The last case is the already-normalized one: it must come back unchanged rather than
        // growing a second OR ALTER.
        StoredProcedure.NormalizeCreateStatement(body).ShouldBe("CREATE OR ALTER PROCEDURE dbo.p AS SELECT 1;");
    }

    [Fact]
    public void a_leading_line_comment_is_kept_and_the_preamble_behind_it_is_rewritten()
    {
        StoredProcedure.NormalizeCreateStatement("-- delete a batch of envelopes\nCREATE PROC dbo.p AS SELECT 1;")
            .ShouldBe("-- delete a batch of envelopes\nCREATE OR ALTER PROCEDURE dbo.p AS SELECT 1;");
    }

    [Fact]
    public void a_leading_block_comment_is_kept_and_the_preamble_behind_it_is_rewritten()
    {
        StoredProcedure.NormalizeCreateStatement("/* owner: wolverine\n   weasel#593 */\ncreate procedure dbo.p AS SELECT 1;")
            .ShouldBe("/* owner: wolverine\n   weasel#593 */\nCREATE OR ALTER PROCEDURE dbo.p AS SELECT 1;");
    }

    [Fact]
    public void the_same_words_inside_a_later_string_literal_are_left_alone()
    {
        StoredProcedure.NormalizeCreateStatement("CREATE PROC dbo.p AS SELECT 'CREATE PROCEDURE';")
            .ShouldBe("CREATE OR ALTER PROCEDURE dbo.p AS SELECT 'CREATE PROCEDURE';");
    }

    [Fact]
    public void a_removed_procedure_writes_nothing_on_either_path()
    {
        var procedure = new StoredProcedure(theIdentifier, OneLineBody) { IsRemoved = true };

        writeCreate(procedure).ShouldBeEmpty();
        writeCreateOrAlter(procedure).ShouldBeEmpty();
    }

    [Fact]
    public void the_go_lines_are_written_by_the_ddl_and_never_reach_the_body()
    {
        // BodyText and CanonicizeSql feed the delta comparison against sys.sql_modules, which
        // holds the statement and nothing else. A GO in there would never match.
        var procedure = new StoredProcedure(theIdentifier, OneLineBody);

        procedure.BodyText().ShouldNotContain("GO");
        procedure.CanonicizeSql().ShouldNotContain("GO");
    }
}
