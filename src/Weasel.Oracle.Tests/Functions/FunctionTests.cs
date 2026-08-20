using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Functions;
using Xunit;

namespace Weasel.Oracle.Tests.Functions;

/// <summary>
///     Oracle stored functions (weasel#482) — the half of weasel#450 that closed on its views and
///     left this behind.
/// </summary>
/// <remarks>
///     Oracle keeps the source verbatim in <c>all_source</c>, but without the
///     <c>CREATE OR REPLACE</c> wrapper and without the schema qualifier, and with a tab where the
///     caller wrote a space. Both sides of the comparison are reduced to that form.
/// </remarks>
[Collection("integration")]
public class FunctionTests: IntegrationContext
{
    private const string SchemaName = "WEASEL";

    public FunctionTests(): base(SchemaName)
    {
    }

    private static Function NewFunction(int factor = 2) => new(
        $"{SchemaName}.fn_double",
        $@"CREATE OR REPLACE FUNCTION {SchemaName}.fn_double(n IN NUMBER) RETURN NUMBER IS
BEGIN
    RETURN n * {factor};
END;");

    /// <summary>
    ///     What <c>all_source</c> stores, measured rather than assumed: from <c>FUNCTION</c>
    ///     onwards, with the schema qualifier gone because the owner is already the row's own
    ///     column.
    /// </summary>
    [Fact]
    public void the_create_or_replace_prefix_and_the_schema_qualifier_are_stripped()
    {
        Function.StripCreateOrReplace(
                "CREATE OR REPLACE FUNCTION WEASEL.fn_double(n IN NUMBER) RETURN NUMBER IS BEGIN RETURN 1; END;",
                "WEASEL")
            .ShouldStartWith("FUNCTION fn_double");
    }

    [Fact]
    public async Task a_function_round_trips_and_reports_no_delta()
    {
        await ResetSchema();

        var function = NewFunction();
        await function.ApplyChangesAsync(theConnection);

        (await function.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await function.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_unchanged_function_does_not_report_permanent_drift()
    {
        await ResetSchema();
        await NewFunction().ApplyChangesAsync(theConnection);

        (await NewFunction().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await NewFunction().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     A function that exists but does not work is worse than none, so this calls it.
    /// </summary>
    [Fact]
    public async Task the_function_actually_returns_something()
    {
        await ResetSchema();
        await NewFunction().ApplyChangesAsync(theConnection);

        var answer = await theConnection
            .CreateCommand($"SELECT {SchemaName}.fn_double(21) FROM dual")
            .ExecuteScalarAsync();

        Convert.ToInt32(answer).ShouldBe(42);
    }

    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        await ResetSchema();
        await NewFunction().ApplyChangesAsync(theConnection);

        var changed = NewFunction(3);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await changed.ApplyChangesAsync(theConnection);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        var answer = await theConnection
            .CreateCommand($"SELECT {SchemaName}.fn_double(10) FROM dual")
            .ExecuteScalarAsync();

        Convert.ToInt32(answer).ShouldBe(30);
    }

    [Fact]
    public async Task a_function_marked_for_removal_is_dropped()
    {
        await ResetSchema();
        await NewFunction().ApplyChangesAsync(theConnection);

        var removed = Function.ForRemoval($"{SchemaName}.fn_double");

        (await removed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await removed.ApplyChangesAsync(theConnection);

        (await NewFunction().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    /// <summary>
    ///     Oracle's teardown has enumerated functions since long before anything could create one.
    ///     This arms it.
    /// </summary>
    [Fact]
    public async Task dropping_the_schema_takes_its_functions_with_it()
    {
        await ResetSchema();
        await NewFunction().ApplyChangesAsync(theConnection);

        await ResetSchema();

        (await NewFunction().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
