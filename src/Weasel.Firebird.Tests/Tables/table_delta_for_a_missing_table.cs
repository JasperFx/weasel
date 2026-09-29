using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

/// <summary>
///     weasel#658, as on PostgreSQL, SQL Server and SQLite: a <see cref="TableDelta" /> for a table that
///     is not in the database yet answers every question -- the one case whose answer is unambiguously
///     "yes, there are changes" -- instead of throwing a <see cref="System.NullReferenceException" />.
/// </summary>
/// <remarks>
///     It surfaced through Wolverine's resource check, which calls <c>FindDeltaAsync(...).HasChanges()</c>
///     on its queue tables before they are provisioned and reports any exception as a missing resource.
/// </remarks>
public class table_delta_for_a_missing_table
{
    private static Table theExpectedTable()
    {
        var table = new Table("wolverine_queue");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("body").AddIndex();

        return table;
    }

    [Fact]
    public void the_difference_is_create()
    {
        new TableDelta(theExpectedTable(), null).Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public void has_changes_rather_than_throwing()
    {
        new TableDelta(theExpectedTable(), null).HasChanges().ShouldBeTrue();
    }

    [Fact]
    public void has_changes_even_when_the_table_declares_nothing_at_all()
    {
        // The answer cannot depend on the expected table having anything to be missing, because the
        // table itself is the thing that is missing.
        new TableDelta(new Table("wolverine_queue"), null).HasChanges().ShouldBeTrue();
    }

    [Fact]
    public void every_declared_item_is_missing_and_nothing_is_extra()
    {
        var delta = new TableDelta(theExpectedTable(), null);

        delta.Columns.Missing.Select(x => x.Name).ShouldBe(["id", "body"]);
        delta.Columns.Extras.ShouldBeEmpty();
        delta.Columns.Different.ShouldBeEmpty();

        delta.Indexes.Missing.Count.ShouldBe(1);
        delta.Indexes.Extras.ShouldBeEmpty();

        delta.ForeignKeys.HasChanges().ShouldBeFalse();
    }

    [Fact]
    public void nothing_is_withheld()
    {
        new TableDelta(theExpectedTable(), null).WithheldDrops.ShouldBeEmpty();
    }
}
