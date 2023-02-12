using Shouldly;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

public class TableColumnTests
{
    [Fact]
    public void set_as_null()
    {
        var states = new Table("states");

        states.AddColumn("col1", "varchar").AllowNulls();
        var column = states.Columns.Single();

        column.AllowNulls.ShouldBeTrue();
    }

    [Fact]
    public void set_not_null()
    {
        var states = new Table("states");

        states.AddColumn("col1", "varchar").NotNull();
        var column = states.Columns.Single();

        column.AllowNulls.ShouldBeFalse();
    }

    [Fact]
    public void allow_null_then_not_null()
    {
        var states = new Table("states");

        states.AddColumn("col1", "varchar").NotNull();
        var column = states.Columns.Single();


        // last one wins
        column.AllowNulls.ShouldBeFalse();
    }

    [Fact]
    public void not_null_then_allow_null()
    {
        var states = new Table("states");

        states.AddColumn("col1", "varchar").NotNull().AllowNulls();
        var column = states.Columns.Single();

        column.AllowNulls.ShouldBeTrue();
    }

    [Fact]
    public void determine_the_directive_with_basic_options()
    {
        var states = new Table("states");

        var expression = states.AddColumn("col1", "varchar");
        var column = states.Columns.Single();

        // nothing
        column.Declaration()
            .ShouldBe("NULL");

        expression.AllowNulls();
        column.Declaration().ShouldBe("NULL");

        expression.NotNull();
        column.Declaration().ShouldBe("NOT NULL");
    }

    [Fact]
    public void automatically_allow_nulls_false_if_primary_key()
    {
        var states = new Table("states");

        var expression = states.AddColumn("col1", "varchar").AsPrimaryKey();
        var column = states.Columns.Single();

        column.AllowNulls.ShouldBeFalse();

        // ignore it if _pk
        column.AllowNulls = true;

        column.Declaration().ShouldBe("NOT NULL");
    }

    public static IEnumerable<object[]> TableColumnsCanAdd()
    {
        var table = new Table("people");

        yield return new object[] { table.AddColumn<string>("name1").Column, true };
        yield return new object[] { table.AddColumn<string>("name2").AllowNulls().Column, true };
        yield return new object[] { table.AddColumn<string>("name3").NotNull().Column, false };
        yield return new object[] { table.AddColumn<string>("name4").DefaultValueByString("foo").Column, true };
        yield return new object[]
        {
            table.AddColumn<string>("name5").NotNull().DefaultValueByString("foo").Column, true
        };
    }


    [Theory]
    [MemberData(nameof(TableColumnsCanAdd))]
    public void can_add(TableColumn column, bool canAdd)
    {
        column.CanAdd().ShouldBe(canAdd);
    }

    [Fact]
    public void add_column_sql()
    {
        var table = new Table("people");
        var column = table.AddColumn<string>("name1").NotNull().Column;

        column.AddColumnSql(table)
            .ShouldBe("alter table dbo.people add name1 varchar(100) NOT NULL;");
    }
}
