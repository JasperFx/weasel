using Shouldly;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     Firebird keeps a routine's body as source and its header in the catalog, so a model's statement
///     is parsed into the same parts to be compared with it. A part the parser gets wrong is drift that
///     never converges, or a change that is never seen.
/// </summary>
public class PsqlRoutineTests
{
    private const string Function = """
        CREATE FUNCTION fn_describe (
            a INT NOT NULL,
            "Mixed" VARCHAR(10),
            c VARCHAR(10) CHARACTER SET UTF8 COLLATE UNICODE_CI,
            d NUMERIC(10, 2),
            e d_positive,
            f TYPE OF d_positive,
            g TYPE OF COLUMN fn_source.name,
            h BLOB SUB_TYPE TEXT,
            i DOUBLE PRECISION = 1.5,
            j VARCHAR(20) DEFAULT 'it''s; AS (',
            k BIGINT DEFAULT CAST(1 AS BIGINT))
        RETURNS VARCHAR(200) NOT NULL DETERMINISTIC
        AS
            DECLARE VARIABLE total INTEGER;
        BEGIN
            total = a + 1;
            RETURN "Mixed" || ':' || total;
        END;
        """;

    [Fact]
    public void every_part_of_a_function_header_is_read()
    {
        var routine = PsqlRoutine.Parse(Function, PsqlRoutineKind.Function);

        routine.CatalogName.ShouldBe("FN_DESCRIBE");
        routine.Inputs.Select(x => x.Name).ShouldBe(["A", "Mixed", "C", "D", "E", "F", "G", "H", "I", "J", "K"]);

        routine.Inputs[0].ShouldSatisfyAllConditions(
            x => x.Kind.ShouldBe(PsqlTypeKind.DataType),
            x => x.DataType.ShouldBe(new FirebirdColumnType("INTEGER")),
            x => x.NotNull.ShouldBeTrue());

        routine.Inputs[2].DataType.ShouldBe(new FirebirdColumnType("VARCHAR", 10, CharacterSet: "UTF8"));
        routine.Inputs[2].Collation.ShouldBe("UNICODE_CI");
        routine.Inputs[3].DataType.ShouldBe(new FirebirdColumnType("NUMERIC", Precision: 10, Scale: 2));

        routine.Inputs[4].ShouldSatisfyAllConditions(
            x => x.Kind.ShouldBe(PsqlTypeKind.Domain),
            x => x.Domain.ShouldBe("D_POSITIVE"));
        routine.Inputs[5].ShouldSatisfyAllConditions(
            x => x.Kind.ShouldBe(PsqlTypeKind.TypeOfDomain),
            x => x.Domain.ShouldBe("D_POSITIVE"));
        routine.Inputs[6].ShouldSatisfyAllConditions(
            x => x.Kind.ShouldBe(PsqlTypeKind.TypeOfColumn),
            x => x.Relation.ShouldBe("FN_SOURCE"),
            x => x.Column.ShouldBe("NAME"));

        routine.Inputs[7].DataType.ShouldBe(new FirebirdColumnType("BLOB", SubType: "TEXT"));
        routine.Inputs[8].Default.ShouldBe("1.5");
        routine.Inputs[9].Default.ShouldBe("'it''s; AS ('");
        routine.Inputs[10].Default.ShouldBe("CAST(1 AS BIGINT)");

        routine.Returns!.ShouldSatisfyAllConditions(
            x => x.Name.ShouldBeNull(),
            x => x.DataType.ShouldBe(new FirebirdColumnType("VARCHAR", 200)),
            x => x.NotNull.ShouldBeTrue());
        routine.Deterministic.ShouldBeTrue();
        routine.SqlSecurity.ShouldBeNull();

        routine.Body.ShouldStartWith("DECLARE VARIABLE total INTEGER;");
        routine.Body.ShouldEndWith("END");
    }

    [Theory]
    [InlineData("CREATE FUNCTION")]
    [InlineData("create or alter function")]
    [InlineData("RECREATE FUNCTION")]
    [InlineData("ALTER FUNCTION")]
    [InlineData("CREATE\n  OR   ALTER FUNCTION")]
    public void any_verb_is_written_as_create_or_alter(string verb)
    {
        PsqlRoutine.Parse($"{verb} f RETURNS INTEGER AS BEGIN RETURN 1; END", PsqlRoutineKind.Function)
            .Statement.ShouldBe("CREATE OR ALTER FUNCTION f RETURNS INTEGER AS BEGIN RETURN 1; END");
    }

    [Fact]
    public void a_trailing_semicolon_is_left_off()
    {
        PsqlRoutine.Parse("CREATE PROCEDURE p AS BEGIN END;\n", PsqlRoutineKind.Procedure)
            .Statement.ShouldBe("CREATE OR ALTER PROCEDURE p AS BEGIN END");
    }

    /// <summary>
    ///     The header ends at the first <c>AS</c> that is a keyword of its own -- not one in a literal, a
    ///     comment, a delimited identifier or a nested expression.
    /// </summary>
    [Fact]
    public void as_inside_literals_comments_and_expressions_does_not_end_the_header()
    {
        var routine = PsqlRoutine.Parse("""
            CREATE PROCEDURE p (
                "AS" VARCHAR(5) = 'AS',
                b INTEGER = CAST(1 AS INTEGER)) /* AS */ -- AS
            AS BEGIN END
            """, PsqlRoutineKind.Procedure);

        routine.Inputs.Select(x => x.Name).ShouldBe(["AS", "B"]);
        routine.Inputs[1].Default.ShouldBe("CAST(1 AS INTEGER)");
        routine.Body.ShouldBe("BEGIN END");
    }

    [Fact]
    public void a_procedure_reads_its_inputs_and_its_outputs()
    {
        var routine = PsqlRoutine.Parse("""
            CREATE PROCEDURE sp_stamp (note VARCHAR(40) = 'x', times INTEGER NOT NULL DEFAULT 1)
            RETURNS (id INTEGER, stamped VARCHAR(40) NOT NULL)
            SQL SECURITY DEFINER
            AS BEGIN SUSPEND; END
            """, PsqlRoutineKind.Procedure);

        routine.Inputs.Select(x => x.ToDeclaration()).ShouldBe(
            ["\"NOTE\" VARCHAR(40) DEFAULT 'x'", "\"TIMES\" INTEGER NOT NULL DEFAULT 1"]);
        routine.Outputs.Select(x => x.ToDeclaration()).ShouldBe(
            ["\"ID\" INTEGER", "\"STAMPED\" VARCHAR(40) NOT NULL"]);
        routine.SqlSecurity.ShouldBe("DEFINER");
        routine.Returns.ShouldBeNull();
    }

    [Fact]
    public void a_procedure_without_parameters_has_none()
    {
        var routine = PsqlRoutine.Parse("CREATE PROCEDURE p AS BEGIN END", PsqlRoutineKind.Procedure);

        routine.Inputs.ShouldBeEmpty();
        routine.Outputs.ShouldBeEmpty();
    }

    [Fact]
    public void empty_parentheses_are_no_parameters()
    {
        PsqlRoutine.Parse("CREATE FUNCTION f () RETURNS INTEGER AS BEGIN RETURN 1; END", PsqlRoutineKind.Function)
            .Inputs.ShouldBeEmpty();
    }

    [Theory]
    [InlineData("CREATE FUNCTION fn_x RETURNS INTEGER AS BEGIN RETURN 1; END", "FN_X")]
    [InlineData("CREATE FUNCTION \"fn_x\" RETURNS INTEGER AS BEGIN RETURN 1; END", "fn_x")]
    [InlineData("CREATE FUNCTION \"FN_X\" RETURNS INTEGER AS BEGIN RETURN 1; END", "FN_X")]
    public void the_name_is_read_as_the_catalog_stores_it(string statement, string catalogName)
    {
        PsqlRoutine.Parse(statement, PsqlRoutineKind.Function).CatalogName.ShouldBe(catalogName);
    }

    [Fact]
    public void an_external_routine_is_refused()
    {
        Should.Throw<NotSupportedException>(() => PsqlRoutine.Parse(
            "CREATE FUNCTION f (x INTEGER) RETURNS INTEGER EXTERNAL NAME 'udr!f' ENGINE UDR",
            PsqlRoutineKind.Function));
    }

    [Theory]
    [InlineData("CREATE PROCEDURE p AS BEGIN END")]
    [InlineData("SELECT 1 FROM RDB$DATABASE")]
    [InlineData("CREATE FUNCTION f AS BEGIN END")]
    [InlineData("CREATE FUNCTION f (x INTEGER RETURNS INTEGER AS BEGIN END")]
    [InlineData("CREATE FUNCTION f RETURNS INTEGER BEGIN RETURN 1; END")]
    public void a_statement_that_is_not_a_psql_function_is_refused(string statement)
    {
        Should.Throw<ArgumentException>(() => PsqlRoutine.Parse(statement, PsqlRoutineKind.Function));
    }

    /// <summary>
    ///     <c>FLOAT(p)</c> becomes a double at a precision that depends on the server, so the model is
    ///     parsed for the server it is compared with.
    /// </summary>
    [Fact]
    public void a_float_precision_is_read_for_the_server()
    {
        const string statement = "CREATE FUNCTION f (x FLOAT(10)) RETURNS INTEGER AS BEGIN RETURN 1; END";

        PsqlRoutine.Parse(statement, PsqlRoutineKind.Function, new FirebirdServerVersion(3, 0, 0))
            .Inputs[0].DataType.Name.ShouldBe("DOUBLE PRECISION");
        PsqlRoutine.Parse(statement, PsqlRoutineKind.Function, new FirebirdServerVersion(4, 0, 0))
            .Inputs[0].DataType.Name.ShouldBe("FLOAT");
    }

    private static PsqlParameter parameter(string declaration)
        => PsqlRoutine.Parse($"CREATE PROCEDURE p ({declaration}) AS BEGIN END", PsqlRoutineKind.Procedure).Inputs[0];

    [Theory]
    [InlineData("x INT", "x INTEGER")]
    [InlineData("x VARCHAR(10)", "x VARCHAR(10) CHARACTER SET UTF8")]
    [InlineData("x CHARACTER VARYING(10)", "x VARCHAR(10)")]
    [InlineData("x DECIMAL(10,2)", "x DECIMAL(10, 2)")]
    [InlineData("x BLOB SUB_TYPE 1", "x BLOB SUB_TYPE TEXT")]
    [InlineData("x VARCHAR(10) = 'a'", "x VARCHAR(10) DEFAULT 'a'")]
    [InlineData("x INTEGER DEFAULT   1", "x INTEGER = 1")]
    [InlineData("x d_positive", "x \"D_POSITIVE\"")]
    [InlineData("x VARCHAR(10) COLLATE UNICODE_CI", "x VARCHAR(10) CHARACTER SET UTF8 COLLATE UNICODE_CI")]
    public void a_parameter_is_satisfied_by_the_same_parameter_spelled_differently(string model, string actual)
    {
        parameter(model).IsSatisfiedBy(parameter(actual)).ShouldBeTrue();
    }

    [Theory]
    [InlineData("x INTEGER", "x BIGINT")]
    [InlineData("x VARCHAR(10)", "x VARCHAR(20)")]
    [InlineData("x VARCHAR(10) CHARACTER SET UTF8", "x VARCHAR(10) CHARACTER SET NONE")]
    [InlineData("x VARCHAR(10) COLLATE UNICODE_CI", "x VARCHAR(10)")]
    [InlineData("x INTEGER", "x INTEGER NOT NULL")]
    [InlineData("x INTEGER", "x INTEGER = 1")]
    [InlineData("x INTEGER = 1", "x INTEGER = 2")]
    [InlineData("x VARCHAR(5) = 'a'", "x VARCHAR(5) = 'A'")]
    [InlineData("x INTEGER DEFAULT NULL", "x INTEGER")]
    [InlineData("x d_positive", "x TYPE OF d_positive")]
    [InlineData("x TYPE OF COLUMN t.a", "x TYPE OF COLUMN t.b")]
    [InlineData("x INTEGER", "y INTEGER")]
    public void a_parameter_is_not_satisfied_by_a_different_one(string model, string actual)
    {
        parameter(model).IsSatisfiedBy(parameter(actual)).ShouldBeFalse();
    }

    [Fact]
    public void the_body_compares_ignoring_whitespace_and_case_but_not_literals()
    {
        PsqlLexer.NormalizeSource("BEGIN\n  x = 'a';\nEND").ShouldBe(PsqlLexer.NormalizeSource("begin x = 'a'; end"));
        PsqlLexer.NormalizeSource("BEGIN x = 'a'; END").ShouldNotBe(PsqlLexer.NormalizeSource("BEGIN x = 'A'; END"));
    }
}
