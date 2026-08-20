using MySqlConnector;
using Shouldly;
using Weasel.Core;
using Weasel.MySql.Functions;
using Xunit;

namespace Weasel.MySql.Tests.Functions;

/// <summary>
///     MySQL stored functions (weasel#482) — the half of weasel#450 that closed on its views and
///     left this behind.
/// </summary>
/// <remarks>
///     Root credentials, and not only for the usual schema-permission reason: creating a function
///     needs <c>CREATE ROUTINE</c>, and on a server with binary logging enabled it also needs
///     <c>SUPER</c> or <c>log_bin_trust_function_creators</c>. MySQL refuses otherwise with a
///     message about the SUPER privilege that never mentions functions.
/// </remarks>
[Collection("integration")]
public class FunctionTests: IAsyncLifetime
{
    private const string SchemaName = "weasel_testing";

    private MySqlConnection theConnection = default!;

    public async ValueTask InitializeAsync()
    {
        var builder = new MySqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            UserID = "root", Password = "P@55w0rd", Database = SchemaName
        };

        theConnection = new MySqlConnection(builder.ConnectionString);
        await theConnection.OpenAsync();
        await dropAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await dropAsync();
        await theConnection.CloseAsync();
        await theConnection.DisposeAsync();
    }

    private Task dropAsync()
        => theConnection.CreateCommand($"DROP FUNCTION IF EXISTS `{SchemaName}`.fn_double").ExecuteNonQueryAsync();

    private static Function NewFunction(int factor = 2) => new(
        $"{SchemaName}.fn_double",
        $@"CREATE FUNCTION `{SchemaName}`.fn_double(n INT) RETURNS INT DETERMINISTIC
BEGIN
  RETURN n * {factor};
END");

    [Fact]
    public void the_create_statement_drops_first_so_it_is_idempotent()
    {
        var writer = new StringWriter();
        NewFunction().WriteCreateStatement(new MySqlMigrator(), writer);

        var sql = writer.ToString();
        sql.ShouldContain("DROP FUNCTION IF EXISTS");
        sql.ShouldContain("CREATE FUNCTION");
    }

    /// <summary>
    ///     <c>ROUTINE_DEFINITION</c> stores the body from <c>BEGIN</c> onwards, so the caller's
    ///     whole statement has to be trimmed to that before comparing or every function drifts.
    /// </summary>
    [Fact]
    public void the_stored_body_is_everything_from_begin()
    {
        Function.ExtractBody("CREATE FUNCTION f() RETURNS INT DETERMINISTIC\nBEGIN\n RETURN 1;\nEND")
            .ShouldStartWith("BEGIN");
    }

    [Fact]
    public async Task a_function_round_trips_and_reports_no_delta()
    {
        var function = NewFunction();
        await function.ApplyChangesAsync(theConnection);

        (await function.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await function.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_unchanged_function_does_not_report_permanent_drift()
    {
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
        await NewFunction().ApplyChangesAsync(theConnection);

        var answer = await theConnection.CreateCommand($"SELECT `{SchemaName}`.fn_double(21)").ExecuteScalarAsync();

        Convert.ToInt32(answer).ShouldBe(42);
    }

    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        await NewFunction().ApplyChangesAsync(theConnection);

        var changed = NewFunction(3);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await changed.ApplyChangesAsync(theConnection);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        var answer = await theConnection.CreateCommand($"SELECT `{SchemaName}`.fn_double(10)").ExecuteScalarAsync();
        Convert.ToInt32(answer).ShouldBe(30);
    }

    [Fact]
    public async Task a_function_marked_for_removal_is_dropped()
    {
        await NewFunction().ApplyChangesAsync(theConnection);

        var removed = Function.ForRemoval($"{SchemaName}.fn_double");

        (await removed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await removed.ApplyChangesAsync(theConnection);

        (await NewFunction().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
