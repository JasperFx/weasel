using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Views;
using Xunit;
using OracleTable = Weasel.Oracle.Tables.Table;

namespace Weasel.Oracle.Tests.Views;

/// <summary>
///     Oracle had no view support, despite <c>ViewBase</c> existing in Weasel.Core with two working
///     implementations. See weasel#450.
/// </summary>
/// <remarks>
///     Oracle stores the view text exactly as submitted — <c>all_views.TEXT</c> hands back the
///     caller's own SELECT — so the body diffs the same way SQL Server's and SQLite's do. MySQL
///     canonicalizes instead, which is why its slice of #450 is still open.
/// </remarks>
public class ViewTests: IntegrationContext
{
    private const string SchemaName = "WEASEL";

    public ViewTests(): base(SchemaName)
    {
    }

    private async Task createSourceTableAsync(string name)
    {
        var table = new OracleTable($"{SchemaName}.{name}");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.AddColumn<int>("quantity");
        await CreateSchemaObjectInDatabase(table);
    }

    [Fact]
    public void write_create_statement_uses_create_or_replace()
    {
        var view = new View("WEASEL.simple_view", "select id, name from src");

        var sql = view.ToBasicCreateViewSql();

        // Idempotent, and supported on every Oracle version Weasel targets — DROP VIEW IF EXISTS
        // only arrived in 23c.
        sql.ShouldContain("CREATE OR REPLACE VIEW");
        sql.ShouldContain("AS select id, name from src");
    }

    [Fact]
    public void write_drop_statement_swallows_only_the_missing_view_error()
    {
        var view = new View("WEASEL.droppable", "select 1 from dual");

        var writer = new StringWriter();
        view.WriteDropStatement(new OracleMigrator(), writer);
        var sql = writer.ToString();

        sql.ShouldContain("DROP VIEW");
        sql.ShouldContain("SQLCODE != -942");   // anything else still raises
    }

    [Fact]
    public async Task create_a_view_and_read_it_back()
    {
        await ResetSchema();
        await createSourceTableAsync("view_src");

        var view = new View($"{SchemaName}.active_items", $"select id, name from {SchemaName}.view_src where quantity > 0");
        await CreateSchemaObjectInDatabase(view);

        var existing = await view.FetchExistingAsync(theConnection);

        existing.ShouldNotBeNull();
        View.NormalizeSql(existing!.ViewSql)
            .ShouldBe(View.NormalizeSql($"select id, name from {SchemaName}.view_src where quantity > 0"));
    }

    [Fact]
    public async Task a_view_that_matches_reports_no_delta()
    {
        await ResetSchema();
        await createSourceTableAsync("view_src");

        var view = new View($"{SchemaName}.no_delta_view", $"select id, name from {SchemaName}.view_src");
        await CreateSchemaObjectInDatabase(view);

        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_missing_view_reports_create()
    {
        await ResetSchema();
        await createSourceTableAsync("view_src");

        var view = new View($"{SchemaName}.absent_view", $"select id from {SchemaName}.view_src");

        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        await ResetSchema();
        await createSourceTableAsync("view_src");

        await CreateSchemaObjectInDatabase(
            new View($"{SchemaName}.changing_view", $"select id from {SchemaName}.view_src"));

        var changed = new View($"{SchemaName}.changing_view", $"select id, name from {SchemaName}.view_src");

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await changed.ApplyChangesAsync(theConnection);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task applying_the_same_view_twice_is_a_no_op()
    {
        await ResetSchema();
        await createSourceTableAsync("view_src");

        var view = new View($"{SchemaName}.idempotent_view", $"select id, name from {SchemaName}.view_src");

        await view.ApplyChangesAsync(theConnection);
        await view.ApplyChangesAsync(theConnection);

        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     weasel#465: the Oracle teardown queried procedures, functions, tables and sequences, but
    ///     never <c>all_views</c>. <c>DROP TABLE … CASCADE CONSTRAINTS</c> invalidates a dependent
    ///     view rather than dropping it, so the view survived and the schema was never clean. Latent
    ///     until this slice made views creatable — the same trap SQL Server sprang in weasel#464.
    /// </summary>
    [Fact]
    public async Task dropping_the_schema_takes_its_views_with_it()
    {
        await ResetSchema();
        await createSourceTableAsync("view_src");

        var view = new View($"{SchemaName}.survivor_view", $"select id from {SchemaName}.view_src");
        await CreateSchemaObjectInDatabase(view);

        (await view.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();

        await ResetSchema();

        (await view.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
