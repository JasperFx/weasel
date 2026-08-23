using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     IgnoreIndex is Weasel.Core API honoured by the PostgreSQL and SQLite table deltas. SQL Server
///     ignored it, so an index the caller asked Weasel to leave alone was reported as an extra and
///     dropped by the generated patch.
/// </summary>
public class ignored_indexes: IntegrationContext
{
    public ignored_indexes(): base("ignoring")
    {
    }

    private async Task<Table> tableWithAHandTunedIndex()
    {
        await ResetSchema();

        await theConnection.CreateCommand(
                "create table ignoring.docs (id int not null constraint pk_docs primary key, name varchar(50) not null)")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand("create index ix_hand_tuned on ignoring.docs (name)")
            .ExecuteNonQueryAsync();

        var table = new Table("ignoring.docs");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("name", "varchar(50)").NotNull();

        return table;
    }

    [Fact]
    public async Task an_ignored_index_is_not_drift()
    {
        var table = await tableWithAHandTunedIndex();
        table.IgnoreIndex("ix_hand_tuned");

        var delta = await table.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_ignored_index_is_not_dropped_by_the_patch()
    {
        var table = await tableWithAHandTunedIndex();
        table.IgnoreIndex("ix_hand_tuned");

        var delta = await table.FindDeltaAsync(theConnection);
        var writer = new StringWriter();
        delta.WriteUpdate(new SqlServerMigrator(), writer);

        writer.ToString().ShouldNotContain("ix_hand_tuned");
    }

    [Fact]
    public async Task an_index_that_is_not_ignored_is_still_drift()
    {
        var table = await tableWithAHandTunedIndex();

        var delta = await table.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
    }
}
