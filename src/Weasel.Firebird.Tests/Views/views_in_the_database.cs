using FirebirdSql.Data.FirebirdClient;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Weasel.Firebird.Views;
using Xunit;

namespace Weasel.Firebird.Tests.Views;

/// <summary>
///     Firebird keeps a view's query as it was written, in <c>RDB$VIEW_SOURCE</c>, so the delta compares
///     the two texts ignoring whitespace and case outside literals -- and an update is
///     <c>CREATE OR ALTER VIEW</c>, which keeps whatever depends on the view.
/// </summary>
public class views_in_the_database: IntegrationContext
{
    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();

        var source = new Table("view_src");
        source.AddColumn<int>("id").AsPrimaryKey();
        source.AddColumn<string>("name");
        source.AddColumn<int>("quantity");
        await ApplyAsync(source);

        await ExecuteAsync("INSERT INTO view_src (id, name, quantity) VALUES (1, 'one', 1)");
        await ExecuteAsync("INSERT INTO view_src (id, name, quantity) VALUES (2, 'none', 0)");
    }

    private Task<string?> storedSourceAsync(string viewName)
        => ScalarAsync<string?>($"SELECT RDB$VIEW_SOURCE FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = '{viewName}'");

    [Fact]
    public async Task a_missing_view_reports_create_and_applying_it_converges()
    {
        var view = new View("created_view", "select id, name from view_src");

        (await view.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Create);

        await ApplyAsync(view);

        (await view.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_view_actually_selects()
    {
        await ApplyAsync(new View("positive_view", "select id, name from view_src where quantity > 0"));

        (await ListAsync("SELECT name FROM positive_view")).ShouldBe(["one"]);
    }

    /// <summary>
    ///     The permanent-drift disease of weasel#445 and weasel#446: the second and third checks have to
    ///     say <c>None</c> as well, through the migration path and on its own.
    /// </summary>
    [Fact]
    public async Task an_unchanged_view_does_not_report_permanent_drift()
    {
        var view = new View("stable_view", """

                select id,
                       name   -- the name's column
                from view_src
                where quantity > 0 and name <> 'it''s; odd'

            """);

        await ApplyAsync(view);

        (await DetermineAsync(view)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task reformatting_the_same_query_is_not_a_change()
    {
        await ApplyAsync(new View("reformat_view", "select id, name from view_src where quantity > 0"));

        var reformatted = new View("reformat_view", """
            SELECT id,
                   name
            FROM   view_src
            WHERE  quantity > 0;
            """);

        (await reformatted.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_changed_literal_is_a_change()
    {
        await ApplyAsync(new View("literal_view", "select id from view_src where name = 'one'"));

        (await new View("literal_view", "select id from view_src where name = 'ONE'").FindDeltaAsync(theConnection))
            .Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    /// <summary>
    ///     <c>CREATE OR ALTER VIEW</c> adds, removes, renames and retypes columns in place.
    /// </summary>
    [Theory]
    [InlineData("select id, name from view_src where quantity > 0")]
    [InlineData("select id, name, quantity from view_src")]
    [InlineData("select id as ident, cast(name as varchar(5)) as short_name from view_src")]
    [InlineData("select 'constant' as only_column from rdb$database")]
    public async Task a_changed_body_reports_update_and_applying_it_converges(string changedSql)
    {
        await ApplyAsync(new View("changing_view", "select id, name from view_src"));

        var changed = new View("changing_view", changedSql);
        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        var migration = await ApplyAsync(changed);
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The update alters the view rather than dropping it: a drop would be refused while another
    ///     view and a procedure depend on it. (Firebird keeps the procedure's compiled request, so it goes
    ///     on reading the old query until the metadata cache lets go of it; that it survives and runs is
    ///     what is Weasel's business.)
    /// </summary>
    [Fact]
    public async Task an_update_keeps_what_depends_on_the_view()
    {
        await ApplyAsync(new View("base_view", "select id, name from view_src"));
        await ExecuteAsync("CREATE VIEW dependent_view AS SELECT name FROM base_view");
        await ExecuteAsync("""
            SET TERM ^ ;
            CREATE PROCEDURE dependent_procedure RETURNS (name VARCHAR(255)) AS
            BEGIN
              FOR SELECT name FROM base_view INTO :name DO SUSPEND;
            END
            ^
            SET TERM ; ^
            """);

        var changed = new View("base_view", "select id, name from view_src where quantity > 0");
        await ApplyAsync(changed);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await ListAsync("SELECT name FROM dependent_view")).ShouldBe(["one"]);
        (await ListAsync("SELECT name FROM dependent_procedure")).ShouldContain("one");
    }

    /// <summary>
    ///     The source is read as a blob, whole. A cast to <c>VARCHAR</c> would cut a long query short --
    ///     or fail on it -- and report drift for good.
    /// </summary>
    [Fact]
    public async Task a_long_view_reads_back_whole()
    {
        var columns = string.Join(", ", Enumerable.Range(0, 1000).Select(i => $"id AS c{i}"));
        var view = new View("long_view", $"select {columns} from view_src");

        await ApplyAsync(view);

        (await storedSourceAsync("LONG_VIEW"))!.Length.ShouldBeGreaterThan(8191);
        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_query_ending_in_a_line_comment_is_applied_beside_other_objects()
    {
        var commented = new View("commented_view", "select id from view_src -- every row");
        var other = new View("other_view", "select name from view_src");

        await ApplyAsync(commented, other);

        (await DetermineAsync(commented, other)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task fetch_existing_returns_the_stored_query()
    {
        await ApplyAsync(new View("fetched_view", "  select id, name from view_src where quantity > 0;  "));

        var existing = await new View("fetched_view", "select 1 as x from rdb$database").FetchExistingAsync(theConnection);

        existing.ShouldNotBeNull();
        existing!.ViewSql.ShouldBe("select id, name from view_src where quantity > 0");
    }

    [Fact]
    public async Task rolling_back_an_update_restores_the_previous_query()
    {
        var original = new View("rolled_view", "select id from view_src");
        await ApplyAsync(original);

        var migration = await ApplyAsync(new View("rolled_view", "select id, name from view_src"));
        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await original.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_view_named_like_a_table_is_refused_by_the_server_and_the_table_survives()
    {
        var view = new View("view_src", "select 1 as x from rdb$database");

        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Create);
        await Should.ThrowAsync<FbException>(() => ApplyAsync(view));

        (await ScalarAsync<int>("SELECT COUNT(*) FROM view_src")).ShouldBe(2);
    }

    /// <summary>
    ///     Firebird's teardown enumerates object types by hand (weasel#464, weasel#465), so a view
    ///     created through the model has to go with the schema.
    /// </summary>
    [Fact]
    public async Task dropping_the_schema_takes_its_views_with_it()
    {
        var view = new View("survivor_view", "select id from view_src");
        await ApplyAsync(view);
        await ApplyAsync(new View("view_over_view", "select id from survivor_view"));

        await theConnection.DropSchemaAsync();

        (await view.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await new View("view_over_view", "select 1 as x from rdb$database").ExistsInDatabaseAsync(theConnection))
            .ShouldBeFalse();
    }

    [Fact]
    public async Task the_drop_statement_drops_it_and_runs_again_harmlessly()
    {
        var view = new View("dropped_view", "select id from view_src");
        await ApplyAsync(view);

        await view.DropAsync(theConnection);
        await view.DropAsync(theConnection);

        (await view.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
