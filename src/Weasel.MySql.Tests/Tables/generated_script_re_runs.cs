using MySqlConnector;
using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

/// <summary>
///     weasel#686. A generated creation script has to be re-runnable. MySQL's was not: everything
///     above the foreign key is guarded, so a second run reached the trailing
///     <c>ALTER TABLE … ADD CONSTRAINT</c> and failed with
///     <c>1826, Duplicate foreign key constraint name</c>.
/// </summary>
/// <remarks>
///     <para>
///         MySQL has neither <c>IF NOT EXISTS</c> for a constraint nor an anonymous block to catch
///         the duplicate with, so the guard PostgreSQL and Oracle use (weasel#681) was not available.
///         The foreign key is declared <b>inline</b> in <c>CREATE TABLE</c> instead, the way SQLite
///         already does, which puts it behind the table's own <c>IF NOT EXISTS</c> — no probe, no
///         escaping, and no race between two sessions both reading "absent".
///     </para>
///     <para>
///         That became available only once weasel#677 made the script order its tables by
///         dependency: InnoDB resolves an inline foreign key at <c>CREATE TABLE</c> time, so the
///         referenced table has to exist already.
///     </para>
///     <para>
///         <b>Only <see cref="the_script_runs_and_re_runs" /> fails without the change</b>, and the
///         other two are worth keeping anyway rather than being read as coverage they are not. The
///         round-trip test passes either way because <c>information_schema</c> records the key
///         identically however it was declared — which is the fact that made this approach viable,
///         so it is pinned rather than assumed. The ordering test passes either way because a
///         trailing <c>ALTER</c> never cared what order the tables were created in; it guards the
///         new requirement that the inline form introduces.
///     </para>
/// </remarks>
[Collection("integration")]
public class generated_script_re_runs: IntegrationContext
{
    private const string Schema = "weasel_testing";

    private static Table table(string name, string? references = null)
    {
        var table = new Table(new MySqlObjectName(Schema, name));
        table.AddColumn<int>("id").AsPrimaryKey();

        if (references != null)
        {
            table.AddColumn<int>("p_id").AllowNulls();
            table.ForeignKeys.Add(new ForeignKey($"fk_{name}")
            {
                LinkedTable = new MySqlObjectName(Schema, references),
                ColumnNames = ["p_id"],
                LinkedNames = ["id"]
            });
        }

        return table;
    }

    private static async Task runAsync(string script)
    {
        await using var conn = new MySqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand(script);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<int> foreignKeyCountAsync(string name)
    {
        await using var cmd = theConnection.CreateCommand(
            $"""
             select count(*) from information_schema.TABLE_CONSTRAINTS
             where CONSTRAINT_SCHEMA = '{Schema}' and CONSTRAINT_NAME = '{name}'
                 and CONSTRAINT_TYPE = 'FOREIGN KEY'
             """);

        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    /// <summary>
    ///     The gate for the whole approach: MySQL reads foreign keys back out of
    ///     <c>information_schema</c>, which records them the same way however they were declared. If
    ///     that were not so, moving them inline would have traded a re-runnability bug for a
    ///     permanent false delta, which is much worse.
    /// </summary>
    [Fact]
    public async Task an_inline_foreign_key_round_trips_with_no_delta()
    {
        await ResetSchemaAsync(Schema);

        var parent = table("rt_parent");
        var child = table("rt_child", "rt_parent");

        var db = new DatabaseWithTables(Schema, ConnectionSource.ConnectionString);
        db.AddTable(parent);
        db.AddTable(child);

        await runAsync(db.ToDatabaseScript());

        // Read the table back and compare against the model that produced it
        var delta = await child.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_script_runs_and_re_runs()
    {
        await ResetSchemaAsync(Schema);

        var db = new DatabaseWithTables(Schema, ConnectionSource.ConnectionString);
        db.AddTable(table("rr_parent"));
        db.AddTable(table("rr_child", "rr_parent"));

        var script = db.ToDatabaseScript();

        await runAsync(script);
        (await foreignKeyCountAsync("fk_rr_child")).ShouldBe(1);

        // The assertion with teeth: without the inline declaration this throws 1826
        await runAsync(script);
        await runAsync(script);

        (await foreignKeyCountAsync("fk_rr_child")).ShouldBe(1);
    }

    /// <summary>
    ///     And the ordering the inline form now depends on: InnoDB resolves the reference at
    ///     <c>CREATE TABLE</c> time, so a child declared before its parent would fail outright
    ///     were the script not sorted (weasel#677).
    /// </summary>
    [Fact]
    public async Task a_child_added_before_its_parent_still_runs()
    {
        await ResetSchemaAsync(Schema);

        var db = new DatabaseWithTables(Schema, ConnectionSource.ConnectionString);
        db.AddTable(table("ord_child", "ord_parent"));
        db.AddTable(table("ord_parent"));

        await runAsync(db.ToDatabaseScript());

        (await foreignKeyCountAsync("fk_ord_child")).ShouldBe(1);
    }
}
