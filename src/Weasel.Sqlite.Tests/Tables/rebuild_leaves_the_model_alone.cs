using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
///     Generating a rebuild used to hand the caller's own <see cref="TableColumn" /> instances to
///     the throwaway replacement table, which reparented them onto a table discarded moments later.
/// </summary>
public class rebuild_leaves_the_model_alone
{
    private static Table WidgetsTable(string quantityType)
    {
        var table = new Table("rp_widgets");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("label", "VARCHAR(50)");
        table.AddColumn("quantity", quantityType);
        return table;
    }

    private static async Task<SqliteConnection> openAsync()
    {
        var conn = new SqliteConnection($"Data Source={Path.GetTempFileName()};");
        await conn.OpenAsync();
        await conn.ResetSchemaAsync("main");
        return conn;
    }

    private static async Task<(Table Model, TableDelta Delta)> ARebuildDeltaAsync(SqliteConnection conn)
    {
        await WidgetsTable("REAL").CreateAsync(conn);

        var model = WidgetsTable("numeric");
        var delta = await model.FindDeltaAsync(conn);

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.CanRebuildInPlace.ShouldBeTrue();

        return (model, delta);
    }

    [Fact]
    public async Task the_model_columns_keep_their_parent()
    {
        await using var conn = await openAsync();
        var (model, delta) = await ARebuildDeltaAsync(conn);

        delta.WriteUpdate(new SqliteMigrator(), new StringWriter());

        foreach (var column in model.Columns)
        {
            column.Parent.ShouldBeSameAs(model);
        }
    }

    [Fact]
    public async Task the_rollback_leaves_the_fetched_table_alone()
    {
        await using var conn = await openAsync();
        var (_, delta) = await ARebuildDeltaAsync(conn);

        delta.WriteRollback(new SqliteMigrator(), new StringWriter());

        foreach (var column in delta.Actual!.Columns)
        {
            column.Parent.ShouldBeSameAs(delta.Actual);
        }
    }

    [Fact]
    public async Task a_model_that_generated_a_rebuild_can_still_go_strict()
    {
        await using var conn = await openAsync();
        var (model, delta) = await ARebuildDeltaAsync(conn);

        delta.WriteUpdate(new SqliteMigrator(), new StringWriter());

        model.StrictTypes = true;

        await using var fresh = await openAsync();
        await model.CreateAsync(fresh);

        var cmd = fresh.CreateCommand();
        cmd.CommandText = "select sql from sqlite_master where type = 'table' and name = 'rp_widgets'";
        var sql = (string)(await cmd.ExecuteScalarAsync())!;

        sql.ShouldContain("STRICT");
        sql.ShouldNotContain("VARCHAR");
        sql.ShouldNotContain("numeric");
    }

    [Fact]
    public async Task a_strict_model_that_generated_a_rebuild_reports_no_drift_against_a_matching_database()
    {
        await using var conn = await openAsync();
        var (model, delta) = await ARebuildDeltaAsync(conn);

        delta.WriteUpdate(new SqliteMigrator(), new StringWriter());

        model.StrictTypes = true;

        await using var strict = await openAsync();
        var matching = WidgetsTable("numeric");
        matching.StrictTypes = true;
        await matching.CreateAsync(strict);

        (await matching.FindDeltaAsync(strict)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await model.FindDeltaAsync(strict)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_rebuild_ddl_is_unchanged()
    {
        await using var conn = await openAsync();
        var (_, delta) = await ARebuildDeltaAsync(conn);

        var writer = new StringWriter();
        delta.WriteUpdate(new SqliteMigrator(), writer);

        var lines = writer.ToString()
            .ReplaceLineEndings("\n")
            .Split('\n')
            .Select(x => x.TrimEnd())
            .ToArray();

        lines.ShouldBe([
            "-- Table recreation required due to SQLite ALTER TABLE limitations",
            "",
            "CREATE TABLE IF NOT EXISTS rp_widgets_new (",
            "    id          INTEGER        PRIMARY KEY,",
            "    label       VARCHAR(50)    ,",
            "    quantity    numeric",
            ");",
            "",
            "INSERT INTO rp_widgets_new (id, label, quantity)",
            "SELECT id, label, quantity FROM rp_widgets;",
            "",
            "DROP TABLE rp_widgets;",
            "",
            "ALTER TABLE rp_widgets_new RENAME TO rp_widgets;",
            "",
            ""
        ]);
    }
}
