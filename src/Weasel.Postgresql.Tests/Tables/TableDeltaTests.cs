using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

public class TableDeltaTests
{
    [Fact]
    public void invalid_if_any_new_columns_cannot_be_added()
    {
        var expected = new Table("people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn(new CannotAddColumn("foo", "varchar"));

        var actual = new Table("people");
        actual.AddColumn<int>("id").AsPrimaryKey();

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
    }

    [Fact]
    public void invalid_if_any_new_columns_cannot_be_modified()
    {
        var expected = new Table("people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn(new CannotAddColumn("foo", "varchar"));

        var actual = new Table("people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn(new CannotAddColumn("foo", "int"));

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
    }

    // weasel#399: consumers declare "this wider actual column is acceptable" by overriding the
    // virtual Equals(object) on their own TableColumn subclass — that is the seam Marten uses so
    // an integer mt_version tolerates a column an earlier release already migrated to bigint
    // (marten#4614/#4742). Computed-column detection (weasel#373) routed every column through
    // MatchesForDelta, where a bare Equals(actual) binds to the protected, NON-virtual
    // Equals(TableColumn) overload and skips the override. The column then landed in
    // Columns.Different, so the table was classified Update while the writer emitted nothing —
    // AssertDatabaseMatchesConfiguration threw with an empty change set.
    [Fact]
    public void a_subclass_equality_override_decides_whether_a_column_matches()
    {
        var expected = new Table("people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn(new ToleratesWiderColumn("mt_version", "integer"));

        var actual = new Table("people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn(new TableColumn("mt_version", "bigint"));

        var delta = new TableDelta(expected, actual);

        delta.Columns.Matched.Select(x => x.Name).ShouldContain("mt_version");
        delta.Columns.Different.ShouldBeEmpty();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public void a_subclass_equality_override_that_rejects_still_reports_a_difference()
    {
        var expected = new Table("people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn(new ToleratesWiderColumn("mt_version", "integer"));

        // uuid is NOT the tolerated type, so the override falls through to the base comparison
        var actual = new Table("people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn(new TableColumn("mt_version", "uuid"));

        var delta = new TableDelta(expected, actual);

        delta.Columns.Different.Select(x => x.Expected.Name).ShouldContain("mt_version");
    }

    // Mirrors Marten's RevisionColumn: an integer column that accepts an on-disk bigint rather
    // than emitting a lossy narrowing cast.
    public class ToleratesWiderColumn: TableColumn
    {
        public ToleratesWiderColumn(string name, string type): base(name, type)
        {
        }

        public override bool Equals(object? obj)
        {
            if (obj is TableColumn actual
                && string.Equals(Name, actual.Name, StringComparison.OrdinalIgnoreCase)
                && actual.RawType().Equals("bigint", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            return base.Equals(obj);
        }

        public override int GetHashCode() => base.GetHashCode();
    }

    public class CannotAddColumn: TableColumn
    {
        public CannotAddColumn(string name, string type): base(name, type)
        {
        }


        public override bool CanAdd()
        {
            return false;
        }

        public override bool CanAlter(TableColumn actual)
        {
            return false;
        }
    }
}
