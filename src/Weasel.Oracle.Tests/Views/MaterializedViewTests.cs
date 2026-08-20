using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Tables;
using Weasel.Oracle.Views;
using Xunit;

namespace Weasel.Oracle.Tests.Views;

/// <summary>
///     Oracle materialized views (weasel#453). Not a subclass of <see cref="View" />, unlike
///     PostgreSQL's — Oracle's differ in more than the keyword.
/// </summary>
[Collection("integration")]
public class MaterializedViewTests: IntegrationContext
{
    private const string SchemaName = "WEASEL";

    public MaterializedViewTests(): base(SchemaName)
    {
    }

    private static Table SourceTable()
    {
        var table = new Table($"{SchemaName}.mv_src");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("qty");
        return table;
    }

    private static MaterializedView NewView(string sql = null!) => new(
        $"{SchemaName}.mv_totals",
        sql ?? $"SELECT id, qty FROM {SchemaName}.mv_src");

    [Fact]
    public void refresh_mode_and_query_rewrite_are_emitted()
    {
        var view = NewView();
        view.Refresh = MaterializedViewRefresh.OnCommit;
        view.EnableQueryRewrite = true;

        var writer = new StringWriter();
        view.WriteCreateStatement(new OracleMigrator(), writer);

        var sql = writer.ToString();
        sql.ShouldContain("REFRESH ON COMMIT");
        sql.ShouldContain("ENABLE QUERY REWRITE");
    }

    [Fact]
    public async Task a_materialized_view_round_trips_and_reports_no_delta()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);

        var view = NewView();
        await view.ApplyChangesAsync(theConnection);

        (await view.ExistsInDatabaseAsync(theConnection)).ShouldBeTrue();
        (await view.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_unchanged_view_does_not_report_permanent_drift()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await NewView().ApplyChangesAsync(theConnection);

        (await NewView().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await NewView().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     The point of a materialized view: the rows are stored, not re-evaluated.
    /// </summary>
    [Fact]
    public async Task the_view_holds_rows()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);

        await theConnection.CreateCommand($"INSERT INTO {SchemaName}.mv_src (id, qty) VALUES (1, 5)")
            .ExecuteNonQueryAsync();

        await NewView().ApplyChangesAsync(theConnection);

        var count = await theConnection.CreateCommand($"SELECT COUNT(*) FROM {SchemaName}.mv_totals")
            .ExecuteScalarAsync();

        Convert.ToInt32(count).ShouldBe(1);
    }

    /// <summary>
    ///     There is no <c>CREATE OR REPLACE MATERIALIZED VIEW</c>, so a changed query is
    ///     <see cref="SchemaPatchDifference.Invalid" /> — drop and create, which for a materialized
    ///     view is right: its contents are derived rather than authored.
    /// </summary>
    [Fact]
    public async Task a_changed_query_reports_invalid()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await NewView().ApplyChangesAsync(theConnection);

        var changed = NewView($"SELECT id FROM {SchemaName}.mv_src");

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Invalid);
    }

    /// <summary>
    ///     Oracle lists a materialized view's container table in <c>all_tables</c> under the same
    ///     name, and <c>DROP TABLE</c> against it fails — which is why weasel#465's teardown excludes
    ///     mview containers from its table sweep. This is the test that arms that.
    /// </summary>
    [Fact]
    public async Task dropping_the_schema_takes_the_view_and_its_container_table()
    {
        await ResetSchema();
        await SourceTable().ApplyChangesAsync(theConnection);
        await NewView().ApplyChangesAsync(theConnection);

        await ResetSchema();

        (await NewView().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();

        var leftovers = await theConnection
            .CreateCommand($"SELECT COUNT(*) FROM all_tables WHERE owner = '{SchemaName}'")
            .ExecuteScalarAsync();

        Convert.ToInt32(leftovers).ShouldBe(0);
    }
}
