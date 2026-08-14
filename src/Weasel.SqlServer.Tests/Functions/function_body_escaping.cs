using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Functions;
using Xunit;

namespace Weasel.SqlServer.Tests.Functions;

/// <summary>
///     A function's body is written into an <c>EXEC sp_executesql N'...'</c> string literal, so its
///     own single quotes have to be doubled. They were not, which made any function containing a
///     string literal — a default, a message, a delimiter — impossible to create: the first quote in
///     the body closed the literal and the rest became syntax.
/// </summary>
[Collection("functions")]
public class function_body_escaping: IntegrationContext
{
    private const string BodyWithQuotes = @"
CREATE OR ALTER FUNCTION escaping.greet(@name nvarchar(50)) RETURNS nvarchar(100) AS
BEGIN
    return 'Hello, ' + isnull(@name, 'world') + '!';
END;";

    public function_body_escaping(): base("escaping")
    {
    }

    [Fact]
    public void the_generated_statement_doubles_quotes_from_the_body()
    {
        var function = new Function(new SqlServerObjectName("escaping", "greet"), BodyWithQuotes);

        var writer = new StringWriter();
        function.WriteCreateStatement(new SqlServerMigrator(), writer);
        var sql = writer.ToString();

        // The literal opens once and closes once; every quote from the body is doubled.
        sql.ShouldContain("''Hello, ''");
        sql.ShouldContain("''world''");
        sql.ShouldNotContain("+ 'Hello, ' +");
    }

    [Fact]
    public async Task a_function_whose_body_contains_a_string_literal_can_be_created()
    {
        await ResetSchema();

        var function = new Function(new SqlServerObjectName("escaping", "greet"), BodyWithQuotes);

        // The real proof: before the fix this threw "Unclosed quotation mark after the character
        // string" because the body's own quotes terminated the sp_executesql literal.
        await CreateSchemaObjectInDatabase(function);

        var existing = await Function.FetchExistingAsync(theConnection,
            new SqlServerObjectName("escaping", "greet"));

        existing.ShouldNotBeNull();
    }

    [Fact]
    public async Task the_created_function_actually_runs()
    {
        await ResetSchema();

        await CreateSchemaObjectInDatabase(
            new Function(new SqlServerObjectName("escaping", "greet"), BodyWithQuotes));

        var result = await theConnection.CreateCommand("select escaping.greet('Jakob')")
            .ExecuteScalarAsync();

        // Proves the body survived escaping intact, not merely that something was created.
        result.ShouldBe("Hello, Jakob!");
    }

    [Fact]
    public async Task a_function_with_quotes_reports_no_delta_after_being_applied()
    {
        await ResetSchema();

        var function = new Function(new SqlServerObjectName("escaping", "greet"), BodyWithQuotes);
        await CreateSchemaObjectInDatabase(function);

        var delta = await function.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
