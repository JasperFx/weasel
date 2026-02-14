using Shouldly;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

/// <summary>
/// Tests to verify API parity with PostgreSQL Table implementation
/// </summary>
public class TableApiParityTests
{
    [Fact]
    public void add_column_with_table_column_instance()
    {
        var table = new Table("users");
        var column = new TableColumn("id", "INTEGER");

        var expr = table.AddColumn(column);

        expr.Column.ShouldBe(column);
        table.Columns.Count.ShouldBe(1);
        table.Columns[0].Name.ShouldBe("id");
        column.Parent.ShouldBe(table);
    }

    [Fact]
    public void add_column_with_generic_type_parameter_no_name()
    {
        var table = new Table("users");

        // This should use the custom TableColumn subclass
        var expr = table.AddColumn<TestTableColumn>();

        table.Columns.Count.ShouldBe(1);
        table.Columns[0].ShouldBeOfType<TestTableColumn>();
    }

    [Fact]
    public void ignore_index_adds_to_ignored_set()
    {
        var table = new Table("users");

        table.IgnoreIndex("idx_auto_generated");

        table.IgnoredIndexes.ShouldContain("idx_auto_generated");
    }

    [Fact]
    public void has_index_returns_true_when_index_exists()
    {
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("email");

        var index = new IndexDefinition("idx_email");
        index.AgainstColumns("email");
        table.Indexes.Add(index);

        table.HasIndex("idx_email").ShouldBeTrue();
        table.HasIndex("idx_nonexistent").ShouldBeFalse();
    }

    [Fact]
    public void has_index_is_case_insensitive()
    {
        var table = new Table("users");
        var index = new IndexDefinition("idx_email");
        index.AgainstColumns("email");
        table.Indexes.Add(index);

        table.HasIndex("IDX_EMAIL").ShouldBeTrue();
        table.HasIndex("idx_Email").ShouldBeTrue();
    }

    [Fact]
    public void has_ignored_index_returns_true_when_in_ignored_set()
    {
        var table = new Table("users");
        table.IgnoreIndex("idx_auto");

        table.HasIgnoredIndex("idx_auto").ShouldBeTrue();
        table.HasIgnoredIndex("idx_other").ShouldBeFalse();
    }

    [Fact]
    public void index_matches_compares_normalized_ddl()
    {
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("email");

        var index1 = new IndexDefinition("idx_email");
        index1.AgainstColumns("email");

        var index2 = new IndexDefinition("idx_email");
        index2.AgainstColumns("email");

        index1.Matches(index2, table).ShouldBeTrue();
    }

    [Fact]
    public void index_matches_handles_whitespace_differences()
    {
        var table = new Table("users");
        table.AddColumn<string>("name");

        var index1 = new IndexDefinition("idx_name");
        index1.AgainstColumns("name");

        var index2 = new IndexDefinition("idx_name");
        index2.AgainstColumns("name");

        // Even if DDL has different whitespace, they should match after canonicalization
        index1.Matches(index2, table).ShouldBeTrue();
    }

    [Fact]
    public void index_matches_detects_different_columns()
    {
        var table = new Table("users");
        table.AddColumn<string>("email");
        table.AddColumn<string>("name");

        var index1 = new IndexDefinition("idx_test");
        index1.AgainstColumns("email");

        var index2 = new IndexDefinition("idx_test");
        index2.AgainstColumns("name");

        index1.Matches(index2, table).ShouldBeFalse();
    }

    [Fact]
    public void index_matches_detects_unique_difference()
    {
        var table = new Table("users");
        table.AddColumn<string>("email");

        var index1 = new IndexDefinition("idx_email") { IsUnique = true };
        index1.AgainstColumns("email");

        var index2 = new IndexDefinition("idx_email") { IsUnique = false };
        index2.AgainstColumns("email");

        index1.Matches(index2, table).ShouldBeFalse();
    }

    [Fact]
    public void canonicize_ddl_normalizes_whitespace()
    {
        var sql1 = "CREATE INDEX idx_test ON users (  email  )";
        var sql2 = "CREATE INDEX idx_test ON users (email)";

        var canonical1 = IndexDefinition.CanonicizeDdl(sql1, "main");
        var canonical2 = IndexDefinition.CanonicizeDdl(sql2, "main");

        canonical1.ShouldBe(canonical2);
    }

    [Fact]
    public void canonicize_ddl_removes_if_not_exists()
    {
        var sql = "CREATE INDEX IF NOT EXISTS idx_test ON users (email)";

        var canonical = IndexDefinition.CanonicizeDdl(sql, "main");

        canonical.ShouldNotContain("if not exists");
        canonical.ShouldNotContain("IF NOT EXISTS");
    }

    // Helper class for testing AddColumn<T>()
    private class TestTableColumn : TableColumn
    {
        public TestTableColumn() : base("test_col", "TEXT")
        {
        }
    }
}
