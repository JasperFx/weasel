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

    /// <summary>
    ///     One test per site, deliberately. The two escapes are independent — a name breaks out of
    ///     the sys.schemas literal, OR out of the nested EXEC('...') literal — so a single test that
    ///     only exercises one of them passes while the other is still open. That is exactly how the
    ///     EXEC nesting survived the first round of fixes here.
    /// </summary>
    [Fact]
    public async Task ensure_schema_exists_does_not_execute_an_injected_payload()
    {
        await using var conn = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await conn.CreateCommand(
                "if object_id('dbo.ensure_schema_victim') is null create table dbo.ensure_schema_victim (id int)")
            .ExecuteNonQueryAsync();

        try
        {
            await conn.EnsureSchemaExists("y') PRINT 1; DROP TABLE IF EXISTS dbo.ensure_schema_victim; --");
        }
        catch (Microsoft.Data.SqlClient.SqlException)
        {
        }

        var stillThere = await conn.CreateCommand("select object_id('dbo.ensure_schema_victim')")
            .ExecuteScalarAsync();
        stillThere.ShouldNotBe(DBNull.Value);

        await conn.CreateCommand("drop table if exists dbo.ensure_schema_victim").ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task an_injected_schema_name_does_not_execute_its_payload()
    {
        await using var conn = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await conn.CreateCommand(
                "if object_id('dbo.schema_escaping_victim') is null create table dbo.schema_escaping_victim (id int)")
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

        // Identified rather than counted by name: sys.tables spans every schema in the database, so
        // counting by bare name picks up same-named tables belonging to other tests.
        var stillThere = await conn.CreateCommand("select object_id('dbo.schema_escaping_victim')")
            .ExecuteScalarAsync();
        stillThere.ShouldNotBe(DBNull.Value);

        await conn.CreateCommand("drop table if exists dbo.schema_escaping_victim").ExecuteNonQueryAsync();
    }
}
