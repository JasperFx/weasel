using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Views;
using SqlServerTable = Weasel.SqlServer.Tables.Table;
using Xunit;

namespace Weasel.SqlServer.Tests.Views;

/// <summary>
///     SQL Server had no view support at all, despite <c>ViewBase</c> existing in Weasel.Core with
///     two working implementations. See weasel#450.
/// </summary>
public class ViewTests: IntegrationContext
{
    public ViewTests(): base("views")
    {
    }

    private async Task createSourceTableAsync(string name)
    {
        var table = new SqlServerTable($"views.{name}");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.AddColumn<int>("quantity");
        await CreateSchemaObjectInDatabase(table);
    }

    [Fact]
    public void write_create_statement_wraps_the_create_for_the_batch_rule()
    {
        var view = new View("views.simple", "select id, name from views.source");

        var sql = view.ToBasicCreateViewSql();

        // CREATE VIEW has to be the only statement in its batch, so it goes through sp_executesql.
        sql.ShouldContain("DROP VIEW IF EXISTS views.simple;");
        sql.ShouldContain("EXEC sp_executesql N'CREATE VIEW views.simple AS select id, name from views.source';");
    }

    [Fact]
    public void a_body_containing_a_string_literal_is_escaped_for_the_wrapper()
    {
        var view = new View("views.filtered", "select id from views.source where name = 'active'");

        var sql = view.ToBasicCreateViewSql();

        // Doubled, or the wrapper's own literal closes early — the defect weasel#443 fixed for functions.
        sql.ShouldContain("where name = ''active''");
    }

    [Fact]
    public async Task create_a_view_and_read_it_back()
    {
        await ResetSchema();
        await createSourceTableAsync("source");

        var view = new View("views.active_items", "select id, name from views.source where quantity > 0");
        await CreateSchemaObjectInDatabase(view);

        var existing = await view.FetchExistingAsync(theConnection);

        existing.ShouldNotBeNull();
        View.NormalizeSql(existing!.ViewSql)
            .ShouldBe(View.NormalizeSql("select id, name from views.source where quantity > 0"));
    }

    /// <summary>
    ///     The shape that broke: SQL Server stores a view's text as submitted and puts <c>AS</c> on
    ///     a line of its own, so the first literal <c>" AS "</c> in the stored text is a column
    ///     alias in the SELECT list rather than the view's own separator.
    /// </summary>
    /// <remarks>
    ///     This has to create the view with raw SQL rather than through Weasel: a view Weasel wrote
    ///     has its AS inline, so the defect cannot reproduce from Weasel's own formatting. Reading
    ///     the body back is not enough on its own either -- the truncated body still compared equal
    ///     to itself -- so the body is used to create a second view, which is what actually failed.
    /// </remarks>
    [Fact]
    public async Task a_view_whose_own_as_is_on_its_own_line_reads_back_a_usable_body()
    {
        await ResetSchema();
        await createSourceTableAsync("source");

        await theConnection
            .CreateCommand("CREATE VIEW views.aliased\nAS\nSELECT id, name AS display_name FROM views.source")
            .ExecuteNonQueryAsync();

        var existing = await new View("views.aliased", "select 1").FetchExistingAsync(theConnection);

        existing.ShouldNotBeNull();
        existing!.ViewSql.ShouldContain("SELECT id, name AS display_name");

        await CreateSchemaObjectInDatabase(new View("views.aliased_rebuilt", existing.ViewSql));
    }

    [Fact]
    public async Task a_view_that_matches_reports_no_delta()
    {
        await ResetSchema();
        await createSourceTableAsync("source");

        var view = new View("views.no_delta", "select id, name from views.source");
        await CreateSchemaObjectInDatabase(view);

        var delta = await view.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_missing_view_reports_create()
    {
        await ResetSchema();
        await createSourceTableAsync("source");

        var view = new View("views.not_there_yet", "select id from views.source");

        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        await ResetSchema();
        await createSourceTableAsync("source");

        await CreateSchemaObjectInDatabase(new View("views.changing", "select id from views.source"));

        var changed = new View("views.changing", "select id, name from views.source");

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await changed.ApplyChangesAsync(theConnection);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task applying_the_same_view_twice_is_a_no_op()
    {
        await ResetSchema();
        await createSourceTableAsync("source");

        var view = new View("views.idempotent", "select id, name from views.source");

        await view.ApplyChangesAsync(theConnection);
        await view.ApplyChangesAsync(theConnection);

        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task whitespace_and_case_differences_are_not_drift()
    {
        await ResetSchema();
        await createSourceTableAsync("source");

        await CreateSchemaObjectInDatabase(new View("views.reformatted", "select id, name from views.source"));

        var reformatted = new View("views.reformatted", @"SELECT   id,
    name
FROM views.source");

        (await reformatted.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_view_over_a_join_round_trips()
    {
        await ResetSchema();
        await createSourceTableAsync("source");
        await createSourceTableAsync("other");

        var view = new View("views.joined", @"select s.id, s.name, o.quantity
from views.source s inner join views.other o on o.id = s.id");

        await CreateSchemaObjectInDatabase(view);

        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void move_to_schema_rewraps_the_identifier()
    {
        var view = new View("views.movable", "select 1 as one");

        view.MoveToSchema("other");

        view.Identifier.Schema.ShouldBe("other");
        view.Identifier.ShouldBeOfType<SqlServerObjectName>();
    }
}
