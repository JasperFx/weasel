using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Functions;
using Xunit;

namespace Weasel.Firebird.Tests.Functions;

/// <summary>
///     Firebird keeps a function's body as source and the rest of its header in
///     <c>RDB$FUNCTION_ARGUMENTS</c>, so these tests change one part of the definition at a time and
///     expect each to be seen -- and expect a function written with every kind of parameter to read back
///     as unchanged.
/// </summary>
public class functions_in_the_database: IntegrationContext
{
    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();

        await ExecuteAsync("""
            CREATE DOMAIN d_positive AS INTEGER CHECK (VALUE > 0);
            CREATE TABLE fn_source (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(20) CHARACTER SET UTF8 COLLATE UNICODE_CI);
            """);
    }

    private static Function doubler(int factor = 2, string parameterType = "INTEGER", string returnType = "INTEGER")
        => new("fn_double", $"""
            CREATE FUNCTION fn_double (n {parameterType})
            RETURNS {returnType}
            AS
            BEGIN
              RETURN n * {factor};
            END
            """);

    /// <summary>
    ///     Every kind of parameter Firebird records differently: synonyms, a character set and a
    ///     collation, a domain, <c>TYPE OF</c> a domain and a column, a blob, both spellings of a default
    ///     and <c>DEFAULT NULL</c>, <c>NOT NULL</c>, <c>DETERMINISTIC</c>, declarations, comments and
    ///     literals with semicolons in them.
    /// </summary>
    private const string EveryKindOfParameter = """
        CREATE FUNCTION fn_describe (
            a INT NOT NULL,
            b VARCHAR(10),
            c VARCHAR(10) CHARACTER SET UTF8 COLLATE UNICODE_CI,
            d NUMERIC(10, 2),
            e d_positive,
            f TYPE OF d_positive,
            g TYPE OF COLUMN fn_source.name,
            h BLOB SUB_TYPE TEXT,
            i DOUBLE PRECISION = 1.5,
            j VARCHAR(20) DEFAULT 'it''s; fine',
            k BIGINT DEFAULT NULL)
        RETURNS VARCHAR(200) DETERMINISTIC
        AS
            DECLARE VARIABLE total INTEGER;
        BEGIN
            -- the caller's own text, apostrophe's and all
            total = a + 1;
            RETURN b || ':' || total || '; ' || j;
        END
        """;

    [Fact]
    public async Task a_missing_function_reports_create_and_applying_it_converges()
    {
        var function = doubler();

        (await function.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await function.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Create);

        await ApplyAsync(function);

        (await function.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_function_actually_returns_something()
    {
        await ApplyAsync(doubler());

        (await ScalarAsync<int>("SELECT fn_double(21) FROM RDB$DATABASE")).ShouldBe(42);
    }

    [Fact]
    public async Task a_function_with_every_kind_of_parameter_does_not_report_permanent_drift()
    {
        var function = new Function("fn_describe", EveryKindOfParameter);
        await ApplyAsync(function);

        var delta = await function.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None, delta.ToString());
        (await DetermineAsync(new Function("fn_describe", EveryKindOfParameter))).Difference
            .ShouldBe(SchemaPatchDifference.None);

        (await ScalarAsync<string>("SELECT fn_describe(1, 'x', 'y', 1.25, 3, 4, 'z', 'blob') FROM RDB$DATABASE"))
            .ShouldBe("x:2; it's; fine");
    }

    /// <summary>
    ///     The statement rebuilt from the catalog is a real one: dropped and run again, it makes the same
    ///     function. It is what a rollback runs.
    /// </summary>
    [Fact]
    public async Task fetch_existing_rebuilds_a_statement_that_recreates_the_function()
    {
        var function = new Function("fn_describe", EveryKindOfParameter);
        await ApplyAsync(function);

        var existing = await function.FetchExistingAsync(theConnection);
        existing.ShouldNotBeNull();
        existing!.Statement.ShouldStartWith("CREATE OR ALTER FUNCTION \"FN_DESCRIBE\" (\"A\" INTEGER NOT NULL,");

        await function.DropAsync(theConnection);
        await existing.CreateAsync(theConnection);

        var delta = await function.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None, delta.ToString());
    }

    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        await ApplyAsync(doubler());

        var changed = doubler(3);
        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(changed);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await ScalarAsync<int>("SELECT fn_double(10) FROM RDB$DATABASE")).ShouldBe(30);
    }

    /// <summary>
    ///     The body alone would not show any of these: each is only in the argument catalog.
    /// </summary>
    [Theory]
    [InlineData("INTEGER", "BIGINT", "INTEGER")]
    [InlineData("INTEGER", "INTEGER", "BIGINT")]
    [InlineData("INTEGER", "INTEGER NOT NULL", "INTEGER")]
    [InlineData("INTEGER", "INTEGER = 1", "INTEGER")]
    [InlineData("INTEGER = 1", "INTEGER = 2", "INTEGER")]
    [InlineData("NUMERIC(10, 2)", "NUMERIC(12, 2)", "INTEGER")]
    public async Task a_changed_header_reports_update_and_applying_it_converges(
        string originalType, string changedType, string changedReturn)
    {
        await ApplyAsync(doubler(parameterType: originalType));

        var changed = doubler(parameterType: changedType, returnType: changedReturn);
        var delta = await changed.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(changed);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_added_or_renamed_parameter_is_a_change()
    {
        await ApplyAsync(doubler());

        var added = new Function("fn_double", "CREATE FUNCTION fn_double (n INTEGER, m INTEGER = 1) RETURNS INTEGER AS BEGIN RETURN n * 2; END");
        (await added.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        var renamed = new Function("fn_double", "CREATE FUNCTION fn_double (x INTEGER) RETURNS INTEGER AS BEGIN RETURN x * 2; END");
        (await renamed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task deterministic_is_compared()
    {
        await ApplyAsync(doubler());

        var deterministic = new Function("fn_double",
            "CREATE FUNCTION fn_double (n INTEGER) RETURNS INTEGER DETERMINISTIC AS BEGIN RETURN n * 2; END");
        (await deterministic.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(deterministic);
        (await deterministic.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     SQL SECURITY arrived in Firebird 4. <c>CREATE OR ALTER</c> redefines the whole function, so a
    ///     statement that stops stating it takes it away again, and that is a change too.
    /// </summary>
    [Fact]
    public async Task sql_security_is_compared_on_firebird_4_and_later()
    {
        if (ServerVersion.Major < 4)
        {
            Assert.Skip($"SQL SECURITY arrived in Firebird 4, and this server is Firebird {ServerVersion}");
        }

        await ApplyAsync(doubler());

        var definer = new Function("fn_double",
            "CREATE FUNCTION fn_double (n INTEGER) RETURNS INTEGER SQL SECURITY DEFINER AS BEGIN RETURN n * 2; END");
        (await definer.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(definer);
        (await definer.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        (await doubler().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);
        await ApplyAsync(doubler());
        (await doubler().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     A signature change goes through <c>CREATE OR ALTER</c> while a view still calls the function,
    ///     which a drop would not.
    /// </summary>
    [Fact]
    public async Task an_update_keeps_a_view_that_calls_the_function()
    {
        await ApplyAsync(doubler());
        await ExecuteAsync("CREATE VIEW doubled AS SELECT fn_double(id) AS twice FROM fn_source");

        await ApplyAsync(doubler(parameterType: "BIGINT", returnType: "BIGINT"));

        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'DOUBLED'")).ShouldBe(1);
    }

    [Fact]
    public async Task a_function_marked_for_removal_is_dropped()
    {
        await ApplyAsync(doubler());

        var removed = Function.ForRemoval("fn_double");
        (await removed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await ApplyAsync(removed);

        (await doubler().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await removed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task rolling_back_an_update_restores_the_previous_function()
    {
        await ApplyAsync(doubler());

        var migration = await ApplyAsync(doubler(3, "BIGINT", "BIGINT"));
        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await doubler().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task dropping_the_schema_takes_its_functions_with_it()
    {
        await ApplyAsync(doubler());
        await ApplyAsync(new Function("fn_quadruple",
            "CREATE FUNCTION fn_quadruple (n INTEGER) RETURNS INTEGER AS BEGIN RETURN fn_double(fn_double(n)); END"));

        await theConnection.DropSchemaAsync();

        (await doubler().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await Function.ForRemoval("fn_quadruple").ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
