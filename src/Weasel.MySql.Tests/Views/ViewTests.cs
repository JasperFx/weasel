using MySqlConnector;
using Shouldly;
using Weasel.Core;
using Weasel.MySql.Views;
using Xunit;
using MySqlTable = Weasel.MySql.Tables.Table;

namespace Weasel.MySql.Tests.Views;

/// <summary>
///     MySQL view support, the last slice of weasel#450. It was blocked not on plumbing but on a
///     design question: MySQL is the only provider of the five that does not store the view text it
///     was given, so there is nothing to diff the caller's SQL against.
/// </summary>
/// <remarks>
///     The chosen answer is to let the server canonicalize the expected SQL too — create a
///     throwaway view, read back its <c>VIEW_DEFINITION</c>, drop it — on both the apply and the
///     assert path, so a view never reports differently depending on which call you make. These
///     tests pin the two properties that make that exact:
///     <c>the_probe_renders_what_the_real_view_would</c> and
///     <c>canonicalization_is_idempotent</c>.
/// </remarks>
[Collection("integration")]
public class ViewTests: IAsyncLifetime
{
    private const string SchemaName = "weasel_views";

    private MySqlConnection theConnection = default!;

    public async ValueTask InitializeAsync()
    {
        var builder = new MySqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            // Root, because the `weasel` user CI connects as is granted rights on weasel_testing
            // only and cannot create the schema these views live in. A default database is still
            // needed: MySQL refuses statements that carry no schema context even when every name
            // in them is qualified.
            UserID = "root", Password = "P@55w0rd", Database = "weasel_testing"
        };

        theConnection = new MySqlConnection(builder.ConnectionString);
        await theConnection.OpenAsync();
        await theConnection.ResetSchemaAsync(SchemaName);

        await createSourceTableAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await theConnection.DropSchemaAsync(SchemaName);
        await theConnection.CloseAsync();
        await theConnection.DisposeAsync();
    }

    private async Task createSourceTableAsync()
    {
        var table = new MySqlTable($"{SchemaName}.view_src");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.AddColumn<int>("quantity");

        await table.CreateAsync(theConnection);
    }

    private Task executeAsync(string sql)
        => theConnection.CreateCommand(sql).ExecuteNonQueryAsync();

    private async Task<string?> storedDefinitionAsync(string viewName)
    {
        var result = await theConnection
            .CreateCommand(
                "SELECT view_definition FROM information_schema.VIEWS WHERE table_schema = @schema AND table_name = @name")
            .With("schema", SchemaName)
            .With("name", viewName)
            .ExecuteScalarAsync();

        return result as string;
    }

    [Fact]
    public void write_create_statement_uses_create_or_replace()
    {
        var view = new View($"{SchemaName}.simple_view", "select id, name from `weasel_views`.view_src");

        var sql = view.ToBasicCreateViewSql();

        sql.ShouldContain("CREATE OR REPLACE VIEW");
        sql.ShouldContain("AS select id, name from `weasel_views`.view_src");
    }

    [Fact]
    public void write_drop_statement_is_guarded()
    {
        var view = new View($"{SchemaName}.droppable", "select 1");

        var writer = new StringWriter();
        view.WriteDropStatement(new MySqlMigrator(), writer);

        writer.ToString().ShouldContain("DROP VIEW IF EXISTS");
    }

    /// <summary>
    ///     The property the whole design rests on: a view's own name never appears in its
    ///     <c>VIEW_DEFINITION</c>, so a probe view created under a throwaway name renders
    ///     byte-identically to what the real view would. Without this the probe would only be an
    ///     approximation.
    /// </summary>
    [Fact]
    public async Task the_probe_renders_what_the_real_view_would()
    {
        const string body = "select id, name from `weasel_views`.view_src where quantity > 0";

        await executeAsync($"CREATE OR REPLACE VIEW `{SchemaName}`.real_view AS {body}");

        var view = new View($"{SchemaName}.real_view", body);
        var canonical = await view.CanonicalizeAsync(theConnection);

        canonical.ShouldBe(await storedDefinitionAsync("real_view"));

        // ...and it really is a rewrite, not a passthrough -- otherwise this test proves nothing.
        canonical.ShouldNotBe(body);
        canonical.ShouldContain($"`{SchemaName}`.`view_src`");
    }

    /// <summary>
    ///     The second property: feeding MySQL its own canonical rendering produces that same
    ///     rendering. Without it the comparison could oscillate.
    /// </summary>
    [Fact]
    public async Task canonicalization_is_idempotent()
    {
        var view = new View($"{SchemaName}.idem_view", "select id, name from `weasel_views`.view_src where quantity > 0");

        var once = await view.CanonicalizeAsync(theConnection);
        var twice = await new View($"{SchemaName}.idem_view", once).CanonicalizeAsync(theConnection);

        twice.ShouldBe(once);
    }

    /// <summary>
    ///     An unqualified name in a view body resolves against the session's default schema at
    ///     creation time, and MySQL bakes that resolution into the stored definition. So the probe
    ///     has to run with the same default database as the connection the real view is created on
    ///     — otherwise it canonicalizes to a different table and every such view reports drift.
    /// </summary>
    [Fact]
    public async Task an_unqualified_name_resolves_the_same_way_in_the_probe()
    {
        await executeAsync("CREATE TABLE IF NOT EXISTS `weasel_testing`.unqualified_src (id INT PRIMARY KEY)");

        try
        {
            const string body = "select id from unqualified_src";

            await executeAsync($"CREATE OR REPLACE VIEW `{SchemaName}`.unqualified_view AS {body}");

            var view = new View($"{SchemaName}.unqualified_view", body);

            (await view.CanonicalizeAsync(theConnection))
                .ShouldBe(await storedDefinitionAsync("unqualified_view"));

            (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        }
        finally
        {
            await executeAsync("DROP TABLE IF EXISTS `weasel_testing`.unqualified_src");
        }
    }

    [Fact]
    public async Task the_probe_leaves_nothing_behind()
    {
        var view = new View($"{SchemaName}.tidy_view", "select id from `weasel_views`.view_src");

        await view.CanonicalizeAsync(theConnection);

        var probes = await theConnection
            .CreateCommand(
                "SELECT COUNT(*) FROM information_schema.VIEWS WHERE table_schema = @schema AND table_name LIKE 'weasel_view_probe_%'")
            .With("schema", SchemaName)
            .ExecuteScalarAsync();

        Convert.ToInt32(probes).ShouldBe(0);
    }

    [Fact]
    public async Task a_missing_view_reports_create_and_applying_it_converges()
    {
        var view = new View($"{SchemaName}.created_view", "select id, name from `weasel_views`.view_src");

        (await view.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();

        var delta = await view.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Create);

        await view.ApplyChangesAsync(theConnection);

        (await view.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
    }

    /// <summary>
    ///     The regression this design exists to prevent. Comparing the caller's SQL against MySQL's
    ///     stored rewrite reports <c>Update</c> forever; the second check has to say <c>None</c>.
    /// </summary>
    [Fact]
    public async Task an_unchanged_view_does_not_report_permanent_drift()
    {
        var view = new View($"{SchemaName}.stable_view", "select id, name from `weasel_views`.view_src where quantity > 0");

        await view.ApplyChangesAsync(theConnection);

        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Reformatting the same query must not read as a change either — that is the other half of
    ///     "does not drift". Different whitespace, different case, same meaning.
    /// </summary>
    [Fact]
    public async Task reformatting_the_same_query_is_not_a_change()
    {
        var view = new View($"{SchemaName}.reformat_view", "select id, name from `weasel_views`.view_src where quantity > 0");
        await view.ApplyChangesAsync(theConnection);

        var reformatted = new View(
            $"{SchemaName}.reformat_view",
            """
            SELECT id,
                   name
            FROM   `weasel_views`.view_src
            WHERE  quantity > 0
            """);

        (await reformatted.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_changed_body_reports_update_and_applying_it_converges()
    {
        var original = new View($"{SchemaName}.changing_view", "select id, name from `weasel_views`.view_src");
        await original.ApplyChangesAsync(theConnection);

        var changed = new View($"{SchemaName}.changing_view", "select id, name, quantity from `weasel_views`.view_src");

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await changed.ApplyChangesAsync(theConnection);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task fetch_existing_returns_the_stored_definition()
    {
        var view = new View($"{SchemaName}.fetched_view", "select id, name from `weasel_views`.view_src where quantity > 0");
        await view.ApplyChangesAsync(theConnection);

        var existing = await view.FetchExistingAsync(theConnection);

        existing.ShouldNotBeNull();
        existing!.ViewSql.ShouldBe(await storedDefinitionAsync("fetched_view"));
    }

    [Fact]
    public async Task dropping_the_schema_takes_its_views_with_it()
    {
        var view = new View($"{SchemaName}.survivor_view", "select id from `weasel_views`.view_src");
        await view.ApplyChangesAsync(theConnection);

        (await view.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();

        await theConnection.ResetSchemaAsync(SchemaName);

        (await view.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }
}
