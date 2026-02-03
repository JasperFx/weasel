using Shouldly;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

public class IndexDefinitionTests
{
    [Fact]
    public void create_index_with_name()
    {
        var index = new IndexDefinition("idx_test");
        index.Name.ShouldBe("idx_test");
    }

    [Fact]
    public void default_index_type_is_btree()
    {
        var index = new IndexDefinition("idx_test");
        index.IndexType.ShouldBe(MySqlIndexType.BTree);
    }

    [Fact]
    public void default_sort_order_is_asc()
    {
        var index = new IndexDefinition("idx_test");
        index.SortOrder.ShouldBe(SortOrder.Asc);
    }

    [Fact]
    public void default_is_not_unique()
    {
        var index = new IndexDefinition("idx_test");
        index.IsUnique.ShouldBeFalse();
    }

    [Fact]
    public void set_columns()
    {
        var index = new IndexDefinition("idx_test")
        {
            Columns = new[] { "email", "name" }
        };

        index.Columns.ShouldBe(new[] { "email", "name" });
    }

    [Fact]
    public void against_columns_fluent()
    {
        var index = new IndexDefinition("idx_test")
            .AgainstColumns("email", "name");

        index.Columns.ShouldBe(new[] { "email", "name" });
    }

    [Fact]
    public void add_column()
    {
        var index = new IndexDefinition("idx_test");
        index.AddColumn("email");
        index.AddColumn("name");

        index.Columns.ShouldBe(new[] { "email", "name" });
    }

    [Fact]
    public void to_ddl_btree_index()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<string>("email");

        var index = new IndexDefinition("idx_people_email")
        {
            Columns = new[] { "email" }
        };

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("CREATE INDEX `idx_people_email`");
        ddl.ShouldContain("ON `weasel_testing`.`people`");
        ddl.ShouldContain("(`email`)");
    }

    [Fact]
    public void to_ddl_unique_index()
    {
        var table = new Table("weasel_testing.people");

        var index = new IndexDefinition("idx_people_email")
        {
            Columns = new[] { "email" },
            IsUnique = true
        };

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("CREATE UNIQUE INDEX");
    }

    [Fact]
    public void to_ddl_hash_index()
    {
        var table = new Table("weasel_testing.people");

        var index = new IndexDefinition("idx_people_email")
        {
            Columns = new[] { "email" },
            IndexType = MySqlIndexType.Hash
        };

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("USING HASH");
    }

    [Fact]
    public void to_ddl_fulltext_index()
    {
        var table = new Table("weasel_testing.articles");

        var index = new IndexDefinition("ft_articles_content")
        {
            Columns = new[] { "content" },
            IndexType = MySqlIndexType.Fulltext
        };

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("CREATE FULLTEXT INDEX");
        ddl.ShouldNotContain("UNIQUE");
    }

    [Fact]
    public void to_ddl_fulltext_with_parser()
    {
        var table = new Table("weasel_testing.articles");

        var index = new IndexDefinition("ft_articles_content")
        {
            Columns = new[] { "content" },
            IndexType = MySqlIndexType.Fulltext,
            FulltextParser = "ngram"
        };

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("WITH PARSER ngram");
    }

    [Fact]
    public void to_ddl_spatial_index()
    {
        var table = new Table("weasel_testing.locations");

        var index = new IndexDefinition("sp_locations_coords")
        {
            Columns = new[] { "coords" },
            IndexType = MySqlIndexType.Spatial
        };

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("CREATE SPATIAL INDEX");
    }

    [Fact]
    public void to_ddl_descending()
    {
        var table = new Table("weasel_testing.people");

        var index = new IndexDefinition("idx_people_email")
        {
            Columns = new[] { "email" },
            SortOrder = SortOrder.Desc
        };

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("`email` DESC");
    }

    [Fact]
    public void to_ddl_with_prefix_length()
    {
        var table = new Table("weasel_testing.people");

        var index = new IndexDefinition("idx_people_email")
        {
            Columns = new[] { "email" },
            PrefixLength = 10
        };

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("`email`(10)");
    }

    [Fact]
    public void to_ddl_multi_column()
    {
        var table = new Table("weasel_testing.people");

        var index = new IndexDefinition("idx_people_name_email")
        {
            Columns = new[] { "first_name", "last_name" }
        };

        var ddl = index.ToDDL(table);

        ddl.ShouldContain("(`first_name`, `last_name`)");
    }

    [Fact]
    public void throws_if_no_columns()
    {
        var table = new Table("weasel_testing.people");
        var index = new IndexDefinition("idx_test");

        Should.Throw<InvalidOperationException>(() => index.ToDDL(table));
    }

    [Fact]
    public void matches_identical_indexes()
    {
        var table = new Table("weasel_testing.people");

        var index1 = new IndexDefinition("idx_test") { Columns = new[] { "email" } };
        var index2 = new IndexDefinition("idx_test") { Columns = new[] { "email" } };

        index1.Matches(index2, table).ShouldBeTrue();
    }

    [Fact]
    public void does_not_match_different_columns()
    {
        var table = new Table("weasel_testing.people");

        var index1 = new IndexDefinition("idx_test") { Columns = new[] { "email" } };
        var index2 = new IndexDefinition("idx_test") { Columns = new[] { "name" } };

        index1.Matches(index2, table).ShouldBeFalse();
    }

    [Fact]
    public void does_not_match_different_uniqueness()
    {
        var table = new Table("weasel_testing.people");

        var index1 = new IndexDefinition("idx_test") { Columns = new[] { "email" }, IsUnique = true };
        var index2 = new IndexDefinition("idx_test") { Columns = new[] { "email" }, IsUnique = false };

        index1.Matches(index2, table).ShouldBeFalse();
    }

    [Fact]
    public void canonicize_ddl_removes_backticks()
    {
        var table = new Table("weasel_testing.people");
        var index = new IndexDefinition("idx_test") { Columns = new[] { "email" } };

        var canonical = IndexDefinition.CanonicizeDdl(index, table);

        canonical.ShouldNotContain("`");
    }
}
