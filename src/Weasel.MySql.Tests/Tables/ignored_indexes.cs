using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

/// <summary>
///     IgnoreIndex is Weasel.Core API, honoured by the PostgreSQL, SQLite and SQL Server table deltas.
///     MySQL ignored it, so an index the caller asked Weasel to leave alone was reported as an extra and
///     dropped by the generated patch.
/// </summary>
public class ignored_indexes: IntegrationContext
{
    private async Task<Table> tableWithAHandTunedIndexAsync(string name)
    {
        await DropTableAsync($"`weasel_testing`.`{name}`");
        await CreateTableAsync(
            $"CREATE TABLE `weasel_testing`.`{name}` (id INT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)");
        await CreateTableAsync($"CREATE INDEX `ix_hand_tuned` ON `weasel_testing`.`{name}` (name)");

        var table = new Table($"weasel_testing.{name}");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("name", "varchar(50)").NotNull();

        return table;
    }

    [Fact]
    public async Task an_ignored_index_is_not_drift()
    {
        var table = await tableWithAHandTunedIndexAsync("ignoring_drift");
        table.IgnoreIndex("ix_hand_tuned");

        var delta = await table.FindDeltaAsync(theConnection, TestContext.Current.CancellationToken);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_ignored_index_is_not_dropped_by_the_patch()
    {
        var table = await tableWithAHandTunedIndexAsync("ignoring_patch");
        table.IgnoreIndex("ix_hand_tuned");
        table.AddColumn("email", "varchar(100)");

        var delta = await table.FindDeltaAsync(theConnection, TestContext.Current.CancellationToken);
        var writer = new StringWriter();
        delta.WriteUpdate(new MySqlMigrator(), writer);

        writer.ToString().ShouldNotContain("ix_hand_tuned");

        await table.ApplyChangesAsync(theConnection, TestContext.Current.CancellationToken);

        var existing = await table.FetchExistingAsync(theConnection, TestContext.Current.CancellationToken);
        existing!.HasIndex("ix_hand_tuned").ShouldBeTrue();
    }

    [Fact]
    public async Task an_ignored_index_is_matched_ignoring_case_as_mysql_matches_it()
    {
        var table = await tableWithAHandTunedIndexAsync("ignoring_case");
        table.IgnoreIndex("IX_Hand_Tuned");

        var delta = await table.FindDeltaAsync(theConnection, TestContext.Current.CancellationToken);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_index_that_is_not_ignored_is_still_drift()
    {
        var table = await tableWithAHandTunedIndexAsync("ignoring_control");

        var delta = await table.FindDeltaAsync(theConnection, TestContext.Current.CancellationToken);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Indexes!.Extras.Select(x => x.Name).ShouldBe(["ix_hand_tuned"]);
    }
}
