using Shouldly;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests.Tables;

public class IndexDefinitionTests
{
    [Fact]
    public void simple_btree_index()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index = new IndexDefinition("idx_people_name")
        {
            Columns = new[] { "name" }
        };

        index.ToDDL(table).ShouldBe("CREATE INDEX WEASEL.idx_people_name ON WEASEL.PEOPLE (name)");
    }

    [Fact]
    public void unique_index()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index = new IndexDefinition("idx_people_email")
        {
            Columns = new[] { "email" },
            IsUnique = true
        };

        index.ToDDL(table).ShouldBe("CREATE UNIQUE INDEX WEASEL.idx_people_email ON WEASEL.PEOPLE (email)");
    }

    [Fact]
    public void descending_index()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index = new IndexDefinition("idx_people_created_at")
        {
            Columns = new[] { "created_at" },
            SortOrder = SortOrder.Desc
        };

        index.ToDDL(table).ShouldBe("CREATE INDEX WEASEL.idx_people_created_at ON WEASEL.PEOPLE (created_at DESC)");
    }

    [Fact]
    public void multi_column_index()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index = new IndexDefinition("idx_people_name_email")
        {
            Columns = new[] { "first_name", "last_name" }
        };

        index.ToDDL(table).ShouldBe("CREATE INDEX WEASEL.idx_people_name_email ON WEASEL.PEOPLE (first_name, last_name)");
    }

    [Fact]
    public void bitmap_index()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index = new IndexDefinition("idx_people_status")
        {
            Columns = new[] { "status" },
            IndexType = OracleIndexType.Bitmap
        };

        index.ToDDL(table).ShouldBe("CREATE BITMAP INDEX WEASEL.idx_people_status ON WEASEL.PEOPLE (status)");
    }

    [Fact]
    public void function_based_index()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index = new IndexDefinition("idx_people_upper_name")
        {
            IndexType = OracleIndexType.FunctionBased,
            FunctionExpression = "UPPER(name)"
        };

        index.ToDDL(table).ShouldBe("CREATE INDEX WEASEL.idx_people_upper_name ON WEASEL.PEOPLE (UPPER(name))");
    }

    [Fact]
    public void index_with_tablespace()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index = new IndexDefinition("idx_people_name")
        {
            Columns = new[] { "name" },
            Tablespace = "USERS"
        };

        index.ToDDL(table).ShouldBe("CREATE INDEX WEASEL.idx_people_name ON WEASEL.PEOPLE (name) TABLESPACE USERS");
    }

    [Fact]
    public void against_columns_fluent()
    {
        var index = new IndexDefinition("idx_test")
            .AgainstColumns("col1", "col2", "col3");

        index.Columns.ShouldBe(new[] { "col1", "col2", "col3" });
    }

    [Fact]
    public void add_column()
    {
        var index = new IndexDefinition("idx_test");
        index.AddColumn("col1");
        index.AddColumn("col2");

        index.Columns.ShouldBe(new[] { "col1", "col2" });
    }

    [Fact]
    public void matches_same_index()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index1 = new IndexDefinition("idx_people_name") { Columns = new[] { "name" } };
        var index2 = new IndexDefinition("idx_people_name") { Columns = new[] { "name" } };

        index1.Matches(index2, table).ShouldBeTrue();
    }

    [Fact]
    public void does_not_match_different_columns()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index1 = new IndexDefinition("idx_people_name") { Columns = new[] { "name" } };
        var index2 = new IndexDefinition("idx_people_name") { Columns = new[] { "email" } };

        index1.Matches(index2, table).ShouldBeFalse();
    }
}
