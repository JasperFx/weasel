using Shouldly;
using Weasel.Core;
using Weasel.Core.Tables;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

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

    public class CannotAddColumn: TableColumn
    {
        public CannotAddColumn(string name, string type): base(name, type)
        {
        }


        public override bool CanAdd()
        {
            return false;
        }

        public override bool CanAlter(TableColumnBase<ColumnCheck> actual)
        {
            return false;
        }
    }
}
