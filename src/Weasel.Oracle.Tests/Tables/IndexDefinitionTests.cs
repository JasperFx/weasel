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

    [Fact]
    public void descending_columns_set_the_direction_per_column()
    {
        var table = new Table("WEASEL.QRTZ_TRIGGERS");
        var index = new IndexDefinition("idx_qrtz_t_nft_st")
        {
            Columns = ["sched_name", "trigger_state", "next_fire_time", "priority", "misfire_instr"]
        };
        index.DescendingColumns.Add("priority");

        index.ToDDL(table).ShouldBe(
            "CREATE INDEX WEASEL.idx_qrtz_t_nft_st ON WEASEL.QRTZ_TRIGGERS "
            + "(sched_name, trigger_state, next_fire_time, priority DESC, misfire_instr)");
    }

    [Fact]
    public void a_whole_index_sort_order_still_renders_one_trailing_desc()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index = new IndexDefinition("idx_test")
        {
            Columns = ["last_name", "first_name"], SortOrder = SortOrder.Desc
        };

        index.ToDDL(table).ShouldEndWith("(last_name, first_name DESC)");
    }

    [Fact]
    public void descending_columns_decide_the_direction_over_a_whole_index_sort_order()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index = new IndexDefinition("idx_test")
        {
            Columns = ["last_name", "first_name"], SortOrder = SortOrder.Desc
        };
        index.DescendingColumns.Add("last_name");

        index.ToDDL(table).ShouldEndWith("(last_name DESC, first_name)");
    }

    [Fact]
    public void a_delimited_descending_column_names_the_same_key_column()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index = new IndexDefinition("idx_test") { Columns = ["next_fire_time", "priority"] };
        index.DescendingColumns.Add("\"PRIORITY\"");

        index.ToDDL(table).ShouldEndWith("(next_fire_time, priority DESC)");
    }

    [Fact]
    public void a_descending_column_that_is_not_a_key_column_is_refused()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index = new IndexDefinition("idx_test") { Columns = ["next_fire_time", "priority"] };
        index.DescendingColumns.Add("priorty");

        Should.Throw<InvalidOperationException>(() => index.ToDDL(table))
            .Message.ShouldContain("priorty");
    }

    [Fact]
    public void descending_columns_on_a_function_expression_are_refused()
    {
        var table = new Table("WEASEL.PEOPLE");
        var index = new IndexDefinition("idx_test")
        {
            IndexType = OracleIndexType.FunctionBased, FunctionExpression = "UPPER(name)"
        };
        index.DescendingColumns.Add("name");

        Should.Throw<InvalidOperationException>(() => index.ToDDL(table));
    }

    [Fact]
    public void direction_is_compared_per_column()
    {
        var table = new Table("WEASEL.PEOPLE");

        // What the reader builds from an index created as (next_fire_time DESC, priority).
        var readBack = new IndexDefinition("idx_test")
        {
            Columns = ["NEXT_FIRE_TIME", "PRIORITY"], SortOrder = SortOrder.Desc
        };
        readBack.DescendingColumns.Add("NEXT_FIRE_TIME");

        var trailing = new IndexDefinition("idx_test")
        {
            Columns = ["next_fire_time", "priority"], SortOrder = SortOrder.Desc
        };
        var firstDescending = new IndexDefinition("idx_test") { Columns = ["next_fire_time", "priority"] };
        firstDescending.DescendingColumns.Add("next_fire_time");

        trailing.Matches(readBack, table).ShouldBeFalse();
        firstDescending.Matches(readBack, table).ShouldBeTrue();
    }
}
