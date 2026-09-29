using FirebirdSql.Data.Isql;
using Shouldly;
using Xunit;

namespace Weasel.Firebird.Tests;

public class FirebirdScriptTests
{
    private static string[] split(string script) => FirebirdScript.Split(script).ToArray();

    [Fact]
    public void splits_plain_statements_on_the_semicolon()
    {
        split("CREATE TABLE A (ID INTEGER);\nCREATE TABLE B (ID INTEGER);\n")
            .ShouldBe(["CREATE TABLE A (ID INTEGER)", "CREATE TABLE B (ID INTEGER)"]);
    }

    [Fact]
    public void an_unterminated_last_statement_is_still_a_statement()
    {
        split("CREATE TABLE A (ID INTEGER);\nDROP TABLE A").ShouldBe(["CREATE TABLE A (ID INTEGER)", "DROP TABLE A"]);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   \n\t ")]
    [InlineData("-- only a comment")]
    [InlineData("/* only a comment ; */")]
    [InlineData(";;\n;")]
    public void nothing_but_whitespace_comments_and_terminators_is_no_statements(string script)
    {
        split(script).ShouldBeEmpty();
    }

    [Fact]
    public void a_comment_only_fragment_between_statements_is_dropped()
    {
        split("CREATE TABLE A (ID INTEGER);\n-- trailing thought\n").ShouldBe(["CREATE TABLE A (ID INTEGER)"]);
    }

    [Fact]
    public void a_leading_comment_travels_with_its_statement()
    {
        split("-- make A\nCREATE TABLE A (ID INTEGER);").ShouldBe(["-- make A\nCREATE TABLE A (ID INTEGER)"]);
    }

    [Fact]
    public void a_semicolon_in_a_string_literal_does_not_end_the_statement()
    {
        split("CREATE TABLE A (S VARCHAR(10) DEFAULT 'a;b');DROP TABLE B;")
            .ShouldBe(["CREATE TABLE A (S VARCHAR(10) DEFAULT 'a;b')", "DROP TABLE B"]);
    }

    [Fact]
    public void a_doubled_quote_does_not_end_the_literal()
    {
        split("INSERT INTO A VALUES ('it''s; fine');DROP TABLE B;")
            .ShouldBe(["INSERT INTO A VALUES ('it''s; fine')", "DROP TABLE B"]);
    }

    [Fact]
    public void a_semicolon_in_a_delimited_identifier_does_not_end_the_statement()
    {
        split("CREATE TABLE \"A;B\" (\"C\"\";D\" INTEGER);DROP TABLE B;")
            .ShouldBe(["CREATE TABLE \"A;B\" (\"C\"\";D\" INTEGER)", "DROP TABLE B"]);
    }

    [Fact]
    public void a_semicolon_in_a_line_comment_does_not_end_the_statement()
    {
        split("CREATE TABLE A ( -- one; two\nID INTEGER);DROP TABLE B;")
            .ShouldBe(["CREATE TABLE A ( -- one; two\nID INTEGER)", "DROP TABLE B"]);
    }

    [Fact]
    public void a_semicolon_in_a_block_comment_does_not_end_the_statement()
    {
        split("CREATE TABLE A (/* one; two */ ID INTEGER);DROP TABLE B;")
            .ShouldBe(["CREATE TABLE A (/* one; two */ ID INTEGER)", "DROP TABLE B"]);
    }

    [Theory]
    [InlineData("q'{it's; quoted}'")]
    [InlineData("Q'(it's; quoted)'")]
    [InlineData("q'[it's; quoted]'")]
    [InlineData("q'<it's; quoted>'")]
    [InlineData("q'!it's; quoted!'")]
    public void a_semicolon_in_an_alternative_string_literal_does_not_end_the_statement(string literal)
    {
        split($"INSERT INTO A VALUES ({literal});DROP TABLE B;")
            .ShouldBe([$"INSERT INTO A VALUES ({literal})", "DROP TABLE B"]);
    }

    [Fact]
    public void a_q_ending_an_identifier_does_not_start_an_alternative_literal()
    {
        split("SELECT seq'x' FROM A;DROP TABLE B;").ShouldBe(["SELECT seq'x' FROM A", "DROP TABLE B"]);
    }

    [Fact]
    public void set_term_switches_the_terminator_and_is_consumed()
    {
        var script = """
                     SET TERM ^ ;
                     CREATE PROCEDURE P AS
                     BEGIN
                       EXIT;
                     END
                     ^
                     SET TERM ; ^
                     DROP TABLE B;
                     """;

        split(script).Select(normalize).ShouldBe(["CREATE PROCEDURE P AS\nBEGIN\n  EXIT;\nEND", "DROP TABLE B"]);
    }

    [Theory]
    [InlineData("set term ^ ;")]
    [InlineData("SET TERM ^;")]
    [InlineData("SET\n  TERM\t^ ;")]
    public void set_term_is_recognised_however_it_is_spaced_or_cased(string directive)
    {
        split($"{directive}\nEXECUTE BLOCK AS BEGIN EXIT; END^\nSET TERM ; ^\nDROP TABLE B;")
            .ShouldBe(["EXECUTE BLOCK AS BEGIN EXIT; END", "DROP TABLE B"]);
    }

    [Fact]
    public void a_terminator_can_be_more_than_one_character()
    {
        split("SET TERM !! ;\nEXECUTE BLOCK AS BEGIN EXIT; END!!\nSET TERM ; !!\nDROP TABLE B;")
            .ShouldBe(["EXECUTE BLOCK AS BEGIN EXIT; END", "DROP TABLE B"]);
    }

    /// <summary>
    ///     isql recognises the directive where a statement starts, and only there. The same words
    ///     inside a literal, or in the middle of a statement, are text.
    /// </summary>
    [Fact]
    public void set_term_is_only_a_directive_where_a_statement_starts()
    {
        split("INSERT INTO A VALUES ('SET TERM ^ ;');DROP TABLE B;")
            .ShouldBe(["INSERT INTO A VALUES ('SET TERM ^ ;')", "DROP TABLE B"]);

        split("UPDATE A SET TERM = 1;DROP TABLE B;").ShouldBe(["UPDATE A SET TERM = 1", "DROP TABLE B"]);
    }

    [Fact]
    public void set_term_after_a_comment_is_still_where_a_statement_starts()
    {
        split("/* switch */ SET TERM ^ ;\nEXECUTE BLOCK AS BEGIN EXIT; END^\nSET TERM ; ^")
            .ShouldBe(["EXECUTE BLOCK AS BEGIN EXIT; END"]);
    }

    [Fact]
    public void a_caret_in_a_literal_comment_or_identifier_does_not_end_a_psql_statement()
    {
        var body = "EXECUTE BLOCK AS\nBEGIN\n  -- a ^ here\n  /* and ^ here */\n  EXECUTE STATEMENT 'SELECT ''^'' FROM \"A^B\"';\nEND";

        split($"SET TERM ^ ;\n{body}\n^\nSET TERM ; ^\n").Select(normalize).ShouldBe([body]);
    }

    [Fact]
    public void carriage_returns_are_whitespace()
    {
        split("CREATE TABLE A (ID INTEGER);\r\n\r\nSET TERM ^ ;\r\nEXECUTE BLOCK AS BEGIN EXIT; END\r\n^\r\nSET TERM ; ^\r\n")
            .ShouldBe(["CREATE TABLE A (ID INTEGER)", "EXECUTE BLOCK AS BEGIN EXIT; END"]);
    }

    [Fact]
    public void parse_reports_which_statements_are_psql()
    {
        var statements = FirebirdScript.Parse("DROP TABLE A;\nSET TERM ^ ;\nEXECUTE BLOCK AS BEGIN EXIT; END^\nSET TERM ; ^\nDROP TABLE B;");

        statements.Select(x => x.IsPsql).ShouldBe([false, true, false]);
    }

    [Fact]
    public void write_statement_terminates_with_a_semicolon()
    {
        var writer = new StringWriter();

        FirebirdScript.WriteStatement(writer, "  DROP TABLE A  ");

        writer.ToString().ShouldBe("DROP TABLE A;" + writer.NewLine);
    }

    [Fact]
    public void write_statement_does_not_double_a_terminator_the_caller_wrote()
    {
        var writer = new StringWriter();

        FirebirdScript.WriteStatement(writer, "DROP TABLE A;");

        writer.ToString().ShouldBe("DROP TABLE A;" + writer.NewLine);
    }

    [Fact]
    public void write_statement_accepts_a_semicolon_inside_a_literal()
    {
        var writer = new StringWriter();

        FirebirdScript.WriteStatement(writer, "ALTER TABLE A ALTER S SET DEFAULT 'a;b'");

        split(writer.ToString()).ShouldBe(["ALTER TABLE A ALTER S SET DEFAULT 'a;b'"]);
    }

    [Fact]
    public void write_statement_refuses_a_semicolon_that_would_end_it_early()
    {
        var ex = Should.Throw<ArgumentException>(() =>
            FirebirdScript.WriteStatement(new StringWriter(), "DROP TABLE A; DROP TABLE B"));

        ex.Message.ShouldContain("WritePsql");
    }

    [Theory]
    [InlineData("")]
    [InlineData("  ;  ")]
    public void write_statement_refuses_an_empty_statement(string statement)
    {
        Should.Throw<ArgumentException>(() => FirebirdScript.WriteStatement(new StringWriter(), statement));
    }

    [Fact]
    public void write_psql_wraps_the_statement_in_set_term()
    {
        var writer = new StringWriter { NewLine = "\n" };

        FirebirdScript.WritePsql(writer, "EXECUTE BLOCK AS BEGIN EXIT; END");

        writer.ToString().ShouldBe("SET TERM ^ ;\nEXECUTE BLOCK AS BEGIN EXIT; END\n^\nSET TERM ; ^\n");
    }

    /// <summary>
    ///     weasel#624: isql would end the statement at the <c>^</c>, truncating the body silently.
    ///     <c>^=</c> is Firebird's own spelling of "not equal", which is how one usually gets in.
    /// </summary>
    [Theory]
    [InlineData("EXECUTE BLOCK AS BEGIN IF (1 ^= 2) THEN EXIT; END")]
    [InlineData("CREATE TRIGGER T FOR A AS BEGIN NEW.X = 1 ^ 2; END")]
    public void write_psql_refuses_a_caret_outside_a_literal(string statement)
    {
        var ex = Should.Throw<ArgumentException>(() => FirebirdScript.WritePsql(new StringWriter(), statement));

        ex.Message.ShouldContain("'^'");
        ex.Message.ShouldContain("<>");
    }

    [Theory]
    [InlineData("EXECUTE BLOCK AS BEGIN EXECUTE STATEMENT 'SELECT ''^'' FROM RDB$DATABASE'; END")]
    [InlineData("EXECUTE BLOCK AS BEGIN /* ^ */ EXIT; END")]
    [InlineData("EXECUTE BLOCK AS BEGIN -- ^\n EXIT; END")]
    [InlineData("EXECUTE BLOCK AS BEGIN EXECUTE STATEMENT 'DROP TABLE \"A^B\"'; END")]
    [InlineData("EXECUTE BLOCK AS BEGIN EXECUTE STATEMENT q'{SELECT '^' FROM RDB$DATABASE}'; END")]
    public void write_psql_accepts_a_caret_inside_a_literal_or_comment(string statement)
    {
        var writer = new StringWriter();

        FirebirdScript.WritePsql(writer, statement);

        split(writer.ToString()).Select(normalize).ShouldBe([normalize(statement)]);
    }

    [Fact]
    public void write_psql_refuses_an_empty_statement()
    {
        Should.Throw<ArgumentException>(() => FirebirdScript.WritePsql(new StringWriter(), "  "));
    }

    [Fact]
    public void what_the_writers_write_splits_back_into_the_same_statements()
    {
        var writer = new StringWriter();
        FirebirdScript.WriteStatement(writer, "CREATE TABLE A (S VARCHAR(10) DEFAULT 'a;b')");
        FirebirdScript.WritePsql(writer, "EXECUTE BLOCK AS BEGIN EXECUTE STATEMENT 'DROP TABLE X'; END");
        FirebirdScript.WriteStatement(writer, "DROP TABLE A");
        FirebirdScript.WritePsql(writer, "CREATE OR ALTER PROCEDURE P AS BEGIN EXIT; END");

        split(writer.ToString()).ShouldBe([
            "CREATE TABLE A (S VARCHAR(10) DEFAULT 'a;b')",
            "EXECUTE BLOCK AS BEGIN EXECUTE STATEMENT 'DROP TABLE X'; END",
            "DROP TABLE A",
            "CREATE OR ALTER PROCEDURE P AS BEGIN EXIT; END"
        ]);
    }

    /// <summary>
    ///     A3: without a commit after each statement, isql runs a guarded block and the plain DDL after
    ///     it in one transaction, the commit fails with 40001, and the guarded object is lost.
    /// </summary>
    [Fact]
    public void the_isql_form_commits_after_every_statement()
    {
        var rendered = new StringWriter();
        FirebirdScript.WriteGuarded(rendered, "SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'F1'",
            "CREATE TABLE F1 (ID INTEGER)");
        FirebirdScript.WriteStatement(rendered, "CREATE TABLE F2 (ID INTEGER)");

        var isql = new StringWriter { NewLine = "\n" };
        FirebirdScript.WriteIsqlScript(isql, rendered.ToString());

        var text = isql.ToString();
        text.ShouldContain("END\n^\nSET TERM ; ^\nCOMMIT;\nCREATE TABLE F2 (ID INTEGER);\nCOMMIT;\n");
        text.Split("COMMIT;").Length.ShouldBe(3);
    }

    [Fact]
    public void the_isql_form_does_not_commit_twice_or_keep_comment_only_fragments()
    {
        var isql = new StringWriter { NewLine = "\n" };

        FirebirdScript.WriteIsqlScript(isql, "DROP TABLE A;\nCOMMIT;\n-- done\n");

        isql.ToString().ShouldBe("DROP TABLE A;\nCOMMIT;\n");
    }

    [Fact]
    public void the_isql_form_splits_back_into_the_same_statements_plus_the_commits()
    {
        var rendered = new StringWriter();
        FirebirdScript.WriteStatement(rendered, "DROP TABLE A");
        FirebirdScript.WritePsql(rendered, "EXECUTE BLOCK AS BEGIN EXIT; END");

        var isql = new StringWriter();
        FirebirdScript.WriteIsqlScript(isql, rendered.ToString());

        split(isql.ToString()).ShouldBe(["DROP TABLE A", "COMMIT", "EXECUTE BLOCK AS BEGIN EXIT; END", "COMMIT"]);
    }

    [Fact]
    public void guarded_runs_the_statement_only_when_the_probe_finds_nothing()
    {
        var block = FirebirdScript.Guarded("SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'A'",
            "CREATE TABLE A (ID INTEGER)");

        block.ShouldBe(
            "EXECUTE BLOCK AS\nBEGIN\n  IF (NOT EXISTS(SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'A')) THEN\n    EXECUTE STATEMENT 'CREATE TABLE A (ID INTEGER)';\nEND");
    }

    [Fact]
    public void guarded_when_exists_runs_the_statement_only_when_the_probe_finds_something()
    {
        var block = FirebirdScript.Guarded("SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'A'",
            "DROP TABLE A", whenExists: true);

        block.ShouldContain("IF (EXISTS(SELECT 1");
        block.ShouldNotContain("NOT EXISTS");
    }

    /// <summary>
    ///     weasel#643: DDL is written into a string literal here, and only here, so this is where its
    ///     quotes are doubled -- once.
    /// </summary>
    [Fact]
    public void guarded_doubles_the_quotes_of_the_statement_once()
    {
        var block = FirebirdScript.Guarded("SELECT 1 FROM RDB$DATABASE",
            "CREATE TABLE A (S VARCHAR(10) DEFAULT 'it''s')");

        block.ShouldContain("EXECUTE STATEMENT 'CREATE TABLE A (S VARCHAR(10) DEFAULT ''it''''s'')';");
    }

    [Fact]
    public void a_long_statement_is_written_as_pieces_joined_with_concatenation()
    {
        var statement = "CREATE TABLE A (" + string.Join(", ",
            Enumerable.Range(0, 2000).Select(i => $"COLUMN_{i:D4} VARCHAR(10) DEFAULT 'x'")) + ")";

        var literal = FirebirdScript.StatementLiteral(statement);

        var pieces = literal.Split(" || ");
        pieces.Length.ShouldBeGreaterThan(1);
        pieces.ShouldAllBe(x => x.StartsWith('\'') && x.EndsWith('\''));
        pieces.Select(x => x[1..^1].Replace("''", "'")).ShouldAllBe(x => x.Length <= FirebirdScript.MaxLiteralPieceLength);
        string.Concat(pieces.Select(x => x[1..^1].Replace("''", "'"))).ShouldBe(statement);
    }

    [Fact]
    public void a_piece_boundary_never_splits_a_doubled_quote()
    {
        var statement = new string('x', FirebirdScript.MaxLiteralPieceLength - 1) + "'" + new string('y', 10);

        var pieces = FirebirdScript.StatementLiteral(statement).Split(" || ");

        pieces.Length.ShouldBe(2);
        pieces[0].ShouldEndWith("x'''");
        string.Concat(pieces.Select(x => x[1..^1].Replace("''", "'"))).ShouldBe(statement);
    }

    [Fact]
    public void a_piece_boundary_never_splits_a_surrogate_pair()
    {
        var statement = new string('x', FirebirdScript.MaxLiteralPieceLength - 1) + "😀" + new string('y', 10);

        var pieces = FirebirdScript.StatementLiteral(statement).Split(" || ");

        pieces.Select(x => x[^2]).ShouldAllBe(x => !char.IsHighSurrogate(x));
        string.Concat(pieces.Select(x => x[1..^1])).ShouldBe(statement);
    }

    [Fact]
    public void a_short_statement_is_one_literal()
    {
        FirebirdScript.StatementLiteral("DROP TABLE A").ShouldBe("'DROP TABLE A'");
    }

    [Fact]
    public void literal_doubles_quotes()
    {
        FirebirdScript.Literal("O'Brien").ShouldBe("'O''Brien'");
    }

    [Fact]
    public void finds_a_token_only_outside_literals_identifiers_and_comments()
    {
        var sql = "'^' \"^\" -- ^\n /* ^ */ q'{^}' ^";

        FirebirdScript.FindOutsideLiterals(sql, "^").ShouldBe(sql.LastIndexOf('^'));
        FirebirdScript.FindOutsideLiterals("'^' \"^\"", "^").ShouldBe(-1);
    }

    /// <summary>
    ///     FirebirdClient's own splitter is not used (see <see cref="FirebirdScript" />), but where it can
    ///     parse a script at all the two have to agree.
    /// </summary>
    [Fact]
    public void agrees_with_fbscript_on_the_scripts_fbscript_can_parse()
    {
        var script = """
                     -- a Weasel-style patch
                     CREATE TABLE PT1 (ID INTEGER NOT NULL, NOTE VARCHAR(50) DEFAULT 'a;b', Q VARCHAR(20) DEFAULT 'it''s', CONSTRAINT PK_PT1 PRIMARY KEY (ID));

                     /* block comment with ; and ^ and ' inside */
                     SET TERM ^ ;
                     EXECUTE BLOCK AS
                     BEGIN
                       -- a line comment; with a semicolon
                       IF (NOT EXISTS(SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'PT2')) THEN
                         EXECUTE STATEMENT 'CREATE TABLE PT2 (ID INTEGER NOT NULL, S VARCHAR(10) DEFAULT ''x;y'', CONSTRAINT PK_PT2 PRIMARY KEY (ID))';
                     END^

                     CREATE OR ALTER PROCEDURE PP1 RETURNS (X VARCHAR(20)) AS
                     BEGIN
                       /* comment ; inside PSQL */
                       X = 'semi;colon';
                       SUSPEND;
                     END^
                     SET TERM ; ^

                     INSERT INTO PT1 (ID) VALUES (1);
                     COMMENT ON TABLE PT1 IS 'a;b ''quoted''';
                     CREATE INDEX IX_PT1 ON PT1 (NOTE);
                     CREATE DESCENDING INDEX IX_PT1_D ON PT1 (ID);
                     ALTER TABLE PT1 ADD CONSTRAINT FK_PT1 FOREIGN KEY (ID) REFERENCES PT1 (ID);
                     CREATE VIEW V_PT AS SELECT ID FROM PT1;
                     COMMIT;
                     DROP TABLE PT1;
                     """;

        var fbScript = new FbScript(script);
        fbScript.Parse();
        var theirs = fbScript.Results.Select(x => normalize(x.Text.Trim())).ToArray();

        split(script).Select(normalize).ShouldBe(theirs);
    }

    private static string normalize(string text) => text.Replace("\r\n", "\n");
}
