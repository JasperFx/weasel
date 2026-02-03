using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

public class TableTests
{
    [Fact]
    public void create_table_with_qualified_name()
    {
        var table = new Table("weasel_testing.people");
        table.Identifier.Schema.ShouldBe("weasel_testing");
        table.Identifier.Name.ShouldBe("people");
    }

    [Fact]
    public void add_column_with_type()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn("name", "VARCHAR(100)");

        table.Columns.Count.ShouldBe(1);
        table.Columns[0].Name.ShouldBe("name");
        table.Columns[0].Type.ShouldBe("VARCHAR(100)");
    }

    [Fact]
    public void add_column_with_generic_type()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<int>("id");
        table.AddColumn<string>("name");

        table.Columns.Count.ShouldBe(2);
        table.Columns[0].Type.ShouldBe("INT");
        table.Columns[1].Type.ShouldBe("VARCHAR(255)");
    }

    [Fact]
    public void add_column_as_primary_key()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<int>("id").AsPrimaryKey();

        table.Columns[0].IsPrimaryKey.ShouldBeTrue();
        table.Columns[0].AllowNulls.ShouldBeFalse();
        table.PrimaryKeyColumns.ShouldBe(new[] { "id" });
    }

    [Fact]
    public void multi_column_primary_key()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("tenant_id").AsPrimaryKey();

        table.PrimaryKeyColumns.Count.ShouldBe(2);
        table.PrimaryKeyColumns.ShouldContain("id");
        table.PrimaryKeyColumns.ShouldContain("tenant_id");
    }

    [Fact]
    public void primary_key_name_is_derived()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<int>("id").AsPrimaryKey();

        table.PrimaryKeyName.ShouldBe("pk_people_id");
    }

    [Fact]
    public void custom_primary_key_name()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.PrimaryKeyName = "my_custom_pk";

        table.PrimaryKeyName.ShouldBe("my_custom_pk");
    }

    [Fact]
    public void has_column()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<string>("name");

        table.HasColumn("name").ShouldBeTrue();
        table.HasColumn("email").ShouldBeFalse();
    }

    [Fact]
    public void column_for()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<string>("name");

        var column = table.ColumnFor("name");
        column.ShouldNotBeNull();
        column.Name.ShouldBe("name");

        table.ColumnFor("nonexistent").ShouldBeNull();
    }

    [Fact]
    public void remove_column()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<string>("name");
        table.AddColumn<string>("email");

        table.RemoveColumn("name");

        table.Columns.Count.ShouldBe(1);
        table.HasColumn("name").ShouldBeFalse();
        table.HasColumn("email").ShouldBeTrue();
    }

    [Fact]
    public void modify_column()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<string>("name");

        var expression = table.ModifyColumn("name");
        expression.ShouldNotBeNull();
        table.ColumnFor("name").ShouldNotBeNull();
    }

    [Fact]
    public void modify_nonexistent_column_throws()
    {
        var table = new Table("weasel_testing.people");

        Should.Throw<ArgumentOutOfRangeException>(() => table.ModifyColumn("nonexistent"));
    }

    [Fact]
    public void add_index()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<string>("email").AddIndex();

        table.Indexes.Count.ShouldBe(1);
        table.Indexes[0].Columns.ShouldBe(new[] { "email" });
    }

    [Fact]
    public void add_index_with_configuration()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<string>("email").AddIndex(x =>
        {
            x.IsUnique = true;
            x.SortOrder = SortOrder.Desc;
        });

        table.Indexes[0].IsUnique.ShouldBeTrue();
        table.Indexes[0].SortOrder.ShouldBe(SortOrder.Desc);
    }

    [Fact]
    public void add_fulltext_index()
    {
        var table = new Table("weasel_testing.articles");
        table.AddColumn("content", "TEXT").AddFulltextIndex();

        table.Indexes.Count.ShouldBe(1);
        table.Indexes[0].IndexType.ShouldBe(MySqlIndexType.Fulltext);
    }

    [Fact]
    public void add_hash_index()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<string>("email").AddIndex(x => x.IndexType = MySqlIndexType.Hash);

        table.Indexes[0].IndexType.ShouldBe(MySqlIndexType.Hash);
    }

    [Fact]
    public void has_index()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<string>("email").AddIndex();

        table.HasIndex("idx_people_email").ShouldBeTrue();
        table.HasIndex("nonexistent").ShouldBeFalse();
    }

    [Fact]
    public void index_for()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<string>("email").AddIndex();

        var index = table.IndexFor("idx_people_email");
        index.ShouldNotBeNull();

        table.IndexFor("nonexistent").ShouldBeNull();
    }

    [Fact]
    public void add_foreign_key()
    {
        var states = new Table("weasel_testing.states");
        var table = new Table("weasel_testing.people");
        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        table.ForeignKeys.Count.ShouldBe(1);
        table.ForeignKeys[0].ColumnNames.ShouldBe(new[] { "state_id" });
        table.ForeignKeys[0].LinkedNames.ShouldBe(new[] { "id" });
    }

    [Fact]
    public void add_foreign_key_with_cascade()
    {
        var states = new Table("weasel_testing.states");
        var table = new Table("weasel_testing.people");
        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id", onDelete: CascadeAction.Cascade);

        table.ForeignKeys[0].OnDelete.ShouldBe(CascadeAction.Cascade);
    }

    [Fact]
    public void find_or_create_foreign_key()
    {
        var table = new Table("weasel_testing.people");

        var fk1 = table.FindOrCreateForeignKey("fk_test");
        var fk2 = table.FindOrCreateForeignKey("fk_test");

        fk1.ShouldBeSameAs(fk2);
        table.ForeignKeys.Count.ShouldBe(1);
    }

    [Fact]
    public void all_names_includes_table_indexes_and_foreign_keys()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("email").AddIndex();
        table.AddColumn<int>("state_id").ForeignKeyTo(new Table("weasel_testing.states"), "id");

        var names = table.AllNames().ToList();
        names.Count.ShouldBe(3);
    }

    [Fact]
    public void partition_by_range()
    {
        var table = new Table("weasel_testing.events");
        table.PartitionByRange("created_at");

        table.PartitionStrategy.ShouldBe(PartitionStrategy.Range);
        table.PartitionExpressions.ShouldContain("created_at");
    }

    [Fact]
    public void partition_by_hash()
    {
        var table = new Table("weasel_testing.events");
        table.PartitionByHash("id");

        table.PartitionStrategy.ShouldBe(PartitionStrategy.Hash);
        table.PartitionExpressions.ShouldContain("id");
    }

    [Fact]
    public void partition_by_list()
    {
        var table = new Table("weasel_testing.events");
        table.PartitionByList("region");

        table.PartitionStrategy.ShouldBe(PartitionStrategy.List);
        table.PartitionExpressions.ShouldContain("region");
    }

    [Fact]
    public void partition_by_key()
    {
        var table = new Table("weasel_testing.events");
        table.PartitionByKey("id");

        table.PartitionStrategy.ShouldBe(PartitionStrategy.Key);
        table.PartitionExpressions.ShouldContain("id");
    }

    [Fact]
    public void clear_partitions()
    {
        var table = new Table("weasel_testing.events");
        table.PartitionByRange("created_at");
        table.ClearPartitions();

        table.PartitionStrategy.ShouldBe(PartitionStrategy.None);
        table.PartitionExpressions.ShouldBeEmpty();
    }

    [Fact]
    public void auto_number_column()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<int>("id").AutoNumber();

        table.Columns[0].IsAutoNumber.ShouldBeTrue();
    }

    [Fact]
    public void default_value_by_string()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<string>("status").DefaultValueByString("active");

        table.Columns[0].DefaultExpression.ShouldBe("'active'");
    }

    [Fact]
    public void default_value_by_int()
    {
        var table = new Table("weasel_testing.people");
        table.AddColumn<int>("count").DefaultValue(0);

        table.Columns[0].DefaultExpression.ShouldBe("0");
    }

    [Fact]
    public void default_engine_is_innodb()
    {
        var table = new Table("weasel_testing.people");
        table.Engine.ShouldBe("InnoDB");
    }

    [Fact]
    public void can_set_custom_engine()
    {
        var table = new Table("weasel_testing.people");
        table.Engine = "MyISAM";
        table.Engine.ShouldBe("MyISAM");
    }

    [Fact]
    public void can_set_charset_and_collation()
    {
        var table = new Table("weasel_testing.people");
        table.Charset = "utf8mb4";
        table.Collation = "utf8mb4_unicode_ci";

        table.Charset.ShouldBe("utf8mb4");
        table.Collation.ShouldBe("utf8mb4_unicode_ci");
    }
}
