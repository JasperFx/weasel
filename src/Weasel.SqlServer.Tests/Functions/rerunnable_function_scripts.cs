using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Functions;
using Xunit;

namespace Weasel.SqlServer.Tests.Functions;

/// <summary>
///     weasel#703: a function's creation script can be run twice, without the delta reporting drift.
/// </summary>
[Collection("rerunnable")]
public class rerunnable_function_scripts: IntegrationContext
{
    private const string Body = """
                                CREATE FUNCTION rerunnable.add_10(@value int) RETURNS int AS
                                BEGIN
                                    RETURN @value + 10;
                                END;
                                """;

    private static readonly SqlServerObjectName Identifier = new("rerunnable", "add_10");

    public rerunnable_function_scripts(): base("rerunnable")
    {
    }

    private static string CreationScriptFor(Function function)
    {
        var writer = new StringWriter();
        function.WriteCreateStatement(new SqlServerMigrator(), writer);
        return writer.ToString();
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    // The failure: a bare CREATE FUNCTION is "There is already an object named 'add_10'" the second time.
    [Fact]
    public async Task a_creation_script_runs_twice()
    {
        await ResetSchema();
        var script = CreationScriptFor(new Function(Identifier, Body));

        await ExecuteAsync(script);
        await ExecuteAsync(script);

        (await Function.FetchExistingAsync(theConnection, Identifier)).ShouldNotBeNull();
    }

    [Fact]
    public async Task the_create_statement_carries_no_drop()
    {
        var function = new Function(Identifier, Body);

        function.CreateStatement().ShouldStartWith("EXEC sp_executesql N'");
        function.CreateStatement().ShouldNotContain("drop function", Case.Insensitive);
        CreationScriptFor(function).ShouldStartWith("drop function if exists rerunnable.add_10;");
    }

    [Fact]
    public async Task no_delta_after_applying()
    {
        await ResetSchema();
        var function = new Function(Identifier, Body);

        await function.ApplyChangesAsync(theConnection);

        (await function.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    // ⚠️ The trap a naive fix walks into. Body() renders THROUGH WriteCreateStatement, so it now includes
    // the drop -- and a function read back from sys.sql_modules always gets the DEFAULT drop. If the delta
    // compared Body(), a function declared with custom drop statements would differ on every pass.
    [Fact]
    public async Task a_function_with_custom_drop_statements_has_no_delta_after_applying()
    {
        await ResetSchema();
        var function = new Function(Identifier, Body, ["DROP FUNCTION IF EXISTS [rerunnable].[add_10];"]);

        await function.ApplyChangesAsync(theConnection);

        (await function.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_changed_body_is_still_an_update()
    {
        await ResetSchema();
        await new Function(Identifier, Body).ApplyChangesAsync(theConnection);

        var changed = new Function(Identifier, Body.Replace("+ 10", "+ 20"));

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);
    }
}
