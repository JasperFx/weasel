using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

/// <summary>
///     The index introspection never read <c>information_schema.STATISTICS.COLLATION</c>, so every index
///     came back ascending. A model with a descending index reported drift on every check and dropped and
///     rebuilt the index on every migration; a database index that was descending where the model is not
///     was never reported at all.
/// </summary>
/// <remarks>
///     MySQL honours a key column's direction from 8.0 on. 5.7 parses <c>DESC</c> and ignores it.
/// </remarks>
public class index_sort_direction: IntegrationContext
{
    private static Table triggersTable(string name)
    {
        var table = new Table($"weasel_testing.{name}");
        table.AddColumn("sched_name", "varchar(120)").NotNull().AsPrimaryKey();
        table.AddColumn("trigger_name", "varchar(150)").NotNull().AsPrimaryKey();
        table.AddColumn("trigger_state", "varchar(16)").NotNull();
        table.AddColumn<long>("next_fire_time");
        table.AddColumn<int>("priority");
        table.AddColumn<int>("misfire_instr");
        return table;
    }

    private async Task<string[]> descendingColumnsInCatalogAsync(string table, string index)
    {
        await using var cmd = theConnection.CreateCommand(
            "SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = 'weasel_testing' "
            + "AND TABLE_NAME = @table AND INDEX_NAME = @index AND COLLATION = 'D' ORDER BY SEQ_IN_INDEX");
        cmd.Parameters.AddWithValue("table", table);
        cmd.Parameters.AddWithValue("index", index);

        var columns = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            columns.Add(reader.GetString(0));
        }

        return columns.ToArray();
    }

    [Fact]
    public async Task a_descending_index_matches_itself_once_created()
    {
        await DropTableAsync("`weasel_testing`.`sort_all_desc`");

        var table = triggersTable("sort_all_desc");
        table.Indexes.Add(new IndexDefinition("idx_sort_all_desc")
        {
            Columns = ["next_fire_time", "priority"], SortOrder = SortOrder.Desc
        });

        await table.CreateAsync(theConnection, TestContext.Current.CancellationToken);

        var delta = await table.FindDeltaAsync(theConnection, TestContext.Current.CancellationToken);

        delta.Indexes!.Different.ShouldBeEmpty();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_descending_index_in_the_database_is_drift_from_an_ascending_model()
    {
        await DropTableAsync("`weasel_testing`.`sort_db_desc`");

        var table = triggersTable("sort_db_desc");
        await table.CreateAsync(theConnection, TestContext.Current.CancellationToken);
        await CreateTableAsync(
            "CREATE INDEX `idx_sort_db_desc` ON `weasel_testing`.`sort_db_desc` "
            + "(`sched_name`, `trigger_state`, `next_fire_time`, `priority` DESC, `misfire_instr`)");

        table.Indexes.Add(new IndexDefinition("idx_sort_db_desc")
        {
            Columns = ["sched_name", "trigger_state", "next_fire_time", "priority", "misfire_instr"]
        });

        var delta = await table.FindDeltaAsync(theConnection, TestContext.Current.CancellationToken);

        delta.Indexes!.Different.Select(x => x.Expected.Name).ShouldBe(["idx_sort_db_desc"]);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task a_mixed_direction_index_is_created_as_declared_and_settles()
    {
        await DropTableAsync("`weasel_testing`.`sort_mixed`");

        var table = triggersTable("sort_mixed");
        var index = new IndexDefinition("idx_sort_mixed")
        {
            Columns = ["sched_name", "trigger_state", "next_fire_time", "priority", "misfire_instr"]
        };
        index.DescendingColumns.Add("priority");
        table.Indexes.Add(index);

        await table.ApplyChangesAsync(theConnection, TestContext.Current.CancellationToken);

        (await descendingColumnsInCatalogAsync("sort_mixed", "idx_sort_mixed")).ShouldBe(["priority"]);
        (await table.FindDeltaAsync(theConnection, TestContext.Current.CancellationToken))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_direction_is_read_back_per_column()
    {
        await DropTableAsync("`weasel_testing`.`sort_read_back`");

        var table = triggersTable("sort_read_back");
        await table.CreateAsync(theConnection, TestContext.Current.CancellationToken);
        await CreateTableAsync(
            "CREATE INDEX `idx_sort_read_back` ON `weasel_testing`.`sort_read_back` "
            + "(`sched_name`, `next_fire_time` DESC, `priority` DESC, `misfire_instr`)");

        var existing = await table.FetchExistingAsync(theConnection, TestContext.Current.CancellationToken);
        var index = existing!.IndexFor("idx_sort_read_back")!;

        index.DescendingColumns.OrderBy(x => x).ShouldBe(["next_fire_time", "priority"]);
        index.ToDDL(existing).ShouldContain(
            "(`sched_name`, `next_fire_time` DESC, `priority` DESC, `misfire_instr`)");
    }

    [Fact]
    public async Task changing_the_direction_rebuilds_the_index_as_declared()
    {
        await DropTableAsync("`weasel_testing`.`sort_changed`");

        var table = triggersTable("sort_changed");
        await table.CreateAsync(theConnection, TestContext.Current.CancellationToken);
        await CreateTableAsync(
            "CREATE INDEX `idx_sort_changed` ON `weasel_testing`.`sort_changed` (`next_fire_time` DESC, `priority`)");

        var index = new IndexDefinition("idx_sort_changed") { Columns = ["next_fire_time", "priority"] };
        index.DescendingColumns.Add("priority");
        table.Indexes.Add(index);

        await table.ApplyChangesAsync(theConnection, TestContext.Current.CancellationToken);

        (await descendingColumnsInCatalogAsync("sort_changed", "idx_sort_changed")).ShouldBe(["priority"]);
        (await table.FindDeltaAsync(theConnection, TestContext.Current.CancellationToken))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
