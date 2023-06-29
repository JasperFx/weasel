using Shouldly;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

public class IndexDefinitionTests
{
    private IndexDefinition theIndex = new IndexDefinition("idx_1")
        .AgainstColumns("column1");

    private Table parent = new Table("people");


    [Fact]
    public void default_sort_order_is_asc()
    {
        theIndex.SortOrder.ShouldBe(SortOrder.Asc);
    }


    [Fact]
    public void is_not_unique_by_default()
    {
        theIndex.IsUnique.ShouldBeFalse();
    }


    [Fact]
    public void write_basic_index()
    {
        theIndex.ToDDL(parent)
            .ShouldBe("CREATE INDEX idx_1 ON dbo.people (column1);");
    }

    [Fact]
    public void write_unique_index()
    {
        theIndex.IsUnique = true;

        theIndex.ToDDL(parent)
            .ShouldBe("CREATE UNIQUE INDEX idx_1 ON dbo.people (column1);");
    }

    [Fact]
    public void write_desc()
    {
        theIndex.SortOrder = SortOrder.Desc;

        theIndex.ToDDL(parent)
            .ShouldBe("CREATE INDEX idx_1 ON dbo.people (column1 DESC);");
    }


    [Fact]
    public void with_a_predicate()
    {
        theIndex.Predicate = "foo > 1";

        theIndex.ToDDL(parent)
            .ShouldBe("CREATE INDEX idx_1 ON dbo.people (column1) WHERE (foo > 1);");
    }

    [Fact]
    public void with_a_non_default_fill_factor()
    {
        theIndex.Predicate = "foo > 1";
        theIndex.FillFactor = 70;

        theIndex.ToDDL(parent)
            .ShouldBe("CREATE INDEX idx_1 ON dbo.people (column1) WHERE (foo > 1) WITH (fillfactor=70);");
    }

    [Fact]
    public void generate_ddl_for_descending_sort_order()
    {
        theIndex.SortOrder = SortOrder.Desc;

        theIndex.ToDDL(parent)
            .ShouldBe("CREATE INDEX idx_1 ON dbo.people (column1 DESC);");
    }

    [Fact]
    public void include_additional_columns()
    {
        theIndex.IncludedColumns = new[]
        {
            "column2"
        };

        theIndex.ToDDL(parent)
            .ShouldBe("CREATE INDEX idx_1 ON dbo.people (column1) INCLUDE (column2);");
    }
}
