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

    [Fact]
    public void write_create_statement_is_guarded()
    {
        var writer = new StringWriter();
        theIndex.WriteCreateStatement(parent, writer);

        writer.ToString().ShouldBe(
            "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_1' AND object_id = OBJECT_ID(N'dbo.people'))"
            + Environment.NewLine
            + "    CREATE INDEX idx_1 ON dbo.people (column1);"
            + Environment.NewLine);
    }

    [Fact]
    public void write_create_statement_escapes_literals()
    {
        var index = new IndexDefinition("o'brien").AgainstColumns("column1");

        var writer = new StringWriter();
        index.WriteCreateStatement(parent, writer);

        writer.ToString().ShouldBe(
            "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'o''brien' AND object_id = OBJECT_ID(N'dbo.people'))"
            + Environment.NewLine
            + "    CREATE INDEX [o'brien] ON dbo.people (column1);"
            + Environment.NewLine);
    }
}
