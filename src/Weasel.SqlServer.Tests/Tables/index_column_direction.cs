using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     SQL Server sets key direction per column. Weasel could only say "the index is descending",
///     which appends one trailing DESC and therefore marks the LAST key column -- so an index read
///     back out of the database as (a DESC, b) was reported as (a, b DESC), a different index, and
///     a model declaring (a, b DESC) compared equal to it.
/// </summary>
public class index_column_direction: IntegrationContext
{
    public index_column_direction(): base("dir")
    {
    }

    private async Task<Table> tableWithIndex(string name, string createIndex)
    {
        await ResetSchema();
        await theConnection.CreateCommand(
                $"create table dir.{name} (id int not null constraint pk_{name} primary key, a int not null, b int not null)")
            .ExecuteNonQueryAsync();
        await theConnection.CreateCommand(createIndex).ExecuteNonQueryAsync();

        var table = new Table($"dir.{name}");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("a").NotNull();
        table.AddColumn<int>("b").NotNull();
        table.PrimaryKeyName = $"pk_{name}";
        return table;
    }

    [Fact]
    public async Task the_fetched_index_reports_the_direction_the_database_has()
    {
        var table = await tableWithIndex("t1", "create index ix_t1 on dir.t1 (a desc, b)");

        var existing = await table.FetchExistingAsync(theConnection);
        var ddl = existing!.IndexFor("ix_t1")!.ToDDL(existing);

        ddl.ShouldContain("(a DESC, b)");
    }

    [Fact]
    public async Task a_model_that_pins_direction_sees_a_differently_directed_index_as_drift()
    {
        var table = await tableWithIndex("t2", "create index ix_t2 on dir.t2 (a desc, b)");
        table.Indexes.Add(new IndexDefinition("ix_t2")
        {
            Columns = ["a", "b"], DescendingColumns = { "b" }, CompareColumnDirection = true
        });

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task a_model_that_pins_the_direction_the_database_has_is_not_drift()
    {
        var table = await tableWithIndex("t3", "create index ix_t3 on dir.t3 (a desc, b)");
        table.Indexes.Add(new IndexDefinition("ix_t3")
        {
            Columns = ["a", "b"], DescendingColumns = { "a" }, CompareColumnDirection = true
        });

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// A model that never stated a direction must not be dragged into comparing one, or every
    /// existing descending index reports drift the user cannot resolve.
    [Fact]
    public async Task a_model_that_says_nothing_about_direction_is_not_drift()
    {
        var table = await tableWithIndex("t4", "create index ix_t4 on dir.t4 (a desc, b)");
        table.Indexes.Add(new IndexDefinition("ix_t4") { Columns = ["a", "b"], SortOrder = SortOrder.Desc });

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_all_descending_index_is_still_not_drift_for_a_coarse_model()
    {
        var table = await tableWithIndex("t5", "create index ix_t5 on dir.t5 (a desc, b desc)");
        table.Indexes.Add(new IndexDefinition("ix_t5") { Columns = ["a", "b"], SortOrder = SortOrder.Desc });

        (await table.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void a_stray_descending_column_is_rejected_rather_than_ignored()
    {
        var table = new Table("dir.stray");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("a").NotNull();
        var index = new IndexDefinition("ix_stray") { Columns = ["a"], DescendingColumns = { "typo" } };
        table.Indexes.Add(index);

        Should.Throw<InvalidOperationException>(() => index.ToDDL(table))
            .Message.ShouldContain("typo");
    }

    [Theory]
    [InlineData("Order", "[Order]")]
    [InlineData("[Order]", "Order")]
    public void a_bracketed_spelling_is_not_mistaken_for_a_stray_column(string keyColumn, string descending)
    {
        var table = new Table("dir.br");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("Order", "int").NotNull();
        var index = new IndexDefinition("ix_br") { Columns = [keyColumn], DescendingColumns = { descending } };
        table.Indexes.Add(index);

        index.ToDDL(table).ShouldContain("[Order] DESC");
    }

    [Fact]
    public void matches_is_symmetric_for_a_given_setting()
    {
        var table = new Table("dir.sym");
        table.AddColumn<int>("a").NotNull();
        table.AddColumn<int>("b").NotNull();

        var pinned = new IndexDefinition("ix") { Columns = ["a", "b"], DescendingColumns = { "a" } };
        var coarse = new IndexDefinition("ix") { Columns = ["a", "b"], SortOrder = SortOrder.Desc };

        pinned.Matches(coarse, table, false).ShouldBe(coarse.Matches(pinned, table, false));
        pinned.Matches(coarse, table, true).ShouldBe(coarse.Matches(pinned, table, true));
        pinned.Matches(coarse, table, true).ShouldBeFalse();
    }
}
