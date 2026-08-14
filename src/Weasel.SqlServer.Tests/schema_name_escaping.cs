using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     Schema names reach two places in one generated statement: a string literal in the
///     <c>sys.schemas</c> lookup, and a bracketed name nested inside <c>EXEC('...')</c> — which is
///     itself a string literal. Both are terminated by <c>'</c>, so both need the quote doubled.
///     Bracketing alone is not enough for the second: it doubles <c>]</c>, not <c>'</c>.
/// </summary>
/// <remarks>
///     Nothing validates a schema name on the way in — <c>DatabaseBase</c> asserts on the object's
///     name, never its schema — and <c>CreateSchemaStatementFor</c> is on the default apply path.
/// </remarks>
public class schema_name_escaping
{
    private const string Payload = "x') PRINT 1; DROP TABLE IF EXISTS dbo.victim; --";

    [Fact]
    public void create_schema_statement_escapes_both_literals()
    {
        var sql = SqlServerMigrator.CreateSchemaStatementFor(Payload);

        // The payload's own quote is doubled everywhere it lands, so it can never close either
        // literal and the DROP stays inert text.
        sql.ShouldNotContain("N'x') PRINT");
        sql.ShouldNotContain("EXEC('CREATE SCHEMA [x')");
        sql.ShouldContain("x'') PRINT");
    }

    [Fact]
    public void drop_schema_statement_escapes_both_literals()
    {
        var writer = new StringWriter();
        new SqlServerMigrator().WriteSchemaDropSql([Payload], writer);
        var sql = writer.ToString();

        sql.ShouldNotContain("N'x') PRINT");
        sql.ShouldNotContain("EXEC('DROP SCHEMA [x')");
        sql.ShouldContain("x'') PRINT");
    }

    [Fact]
    public async Task an_injected_schema_name_does_not_execute_its_payload()
    {
        await using var conn = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await conn.CreateCommand("if object_id('dbo.victim') is null create table dbo.victim (id int)")
            .ExecuteNonQueryAsync();

        // The statement is expected to fail or to create an oddly-named schema; what it must not do
        // is run the payload.
        try
        {
            await conn.CreateCommand(SqlServerMigrator.CreateSchemaStatementFor(Payload)).ExecuteNonQueryAsync();
        }
        catch (Microsoft.Data.SqlClient.SqlException)
        {
        }

        var stillThere = await conn.CreateCommand("select count(*) from sys.tables where name = 'victim'")
            .ExecuteScalarAsync();
        Convert.ToInt32(stillThere).ShouldBe(1);

        await conn.CreateCommand("drop table if exists dbo.victim").ExecuteNonQueryAsync();
    }
}
