using Shouldly;
using Weasel.Core;
using Weasel.Sqlite;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

public class ForeignKeyTests
{
    [Fact]
    public void can_create_simple_foreign_key()
    {
        var fk = new ForeignKey("fk_posts_user_id");
        fk.LinkedTable = new SqliteObjectName("users");
        fk.LinkColumns("user_id", "id");

        var writer = new StringWriter();
        fk.WriteInlineDefinition(writer);
        var sql = writer.ToString();

        sql.ShouldContain("CONSTRAINT");
        sql.ShouldContain("fk_posts_user_id");
        sql.ShouldContain("FOREIGN KEY");
        sql.ShouldContain("user_id");
        sql.ShouldContain("REFERENCES");
        sql.ShouldContain("users");
        sql.ShouldContain("id");
    }

    [Fact]
    public void can_create_composite_foreign_key()
    {
        var fk = new ForeignKey("fk_order_items");
        fk.LinkedTable = new SqliteObjectName("orders");
        fk.LinkColumns("order_id", "id");
        fk.LinkColumns("tenant_id", "tenant_id");

        var writer = new StringWriter();
        fk.WriteInlineDefinition(writer);
        var sql = writer.ToString();

        sql.ShouldContain("order_id");
        sql.ShouldContain("tenant_id");
    }

    [Fact]
    public void can_set_on_delete_cascade()
    {
        var fk = new ForeignKey("fk_posts_user");
        fk.LinkedTable = new SqliteObjectName("users");
        fk.LinkColumns("user_id", "id");
        fk.OnDelete = CascadeAction.Cascade;

        var writer = new StringWriter();
        fk.WriteInlineDefinition(writer);
        var sql = writer.ToString();

        sql.ShouldContain("ON DELETE CASCADE");
    }

    [Fact]
    public void can_set_on_update_set_null()
    {
        var fk = new ForeignKey("fk_posts_category");
        fk.LinkedTable = new SqliteObjectName("categories");
        fk.LinkColumns("category_id", "id");
        fk.OnUpdate = CascadeAction.SetNull;

        var writer = new StringWriter();
        fk.WriteInlineDefinition(writer);
        var sql = writer.ToString();

        sql.ShouldContain("ON UPDATE SET NULL");
    }

    [Fact]
    public void can_set_both_cascade_actions()
    {
        var fk = new ForeignKey("fk_order_items_order");
        fk.LinkedTable = new SqliteObjectName("orders");
        fk.LinkColumns("order_id", "id");
        fk.OnDelete = CascadeAction.Cascade;
        fk.OnUpdate = CascadeAction.Restrict;

        var writer = new StringWriter();
        fk.WriteInlineDefinition(writer);
        var sql = writer.ToString();

        sql.ShouldContain("ON DELETE CASCADE");
        sql.ShouldContain("ON UPDATE RESTRICT");
    }

    [Fact]
    public void read_referential_actions_parses_cascade()
    {
        var fk = new ForeignKey("test_fk");
        fk.ReadReferentialActions("CASCADE", "NO ACTION");

        fk.OnDelete.ShouldBe(CascadeAction.Cascade);
        fk.OnUpdate.ShouldBe(CascadeAction.NoAction);
    }

    [Fact]
    public void read_referential_actions_parses_set_null()
    {
        var fk = new ForeignKey("test_fk");
        fk.ReadReferentialActions("SET NULL", "SET DEFAULT");

        fk.OnDelete.ShouldBe(CascadeAction.SetNull);
        fk.OnUpdate.ShouldBe(CascadeAction.SetDefault);
    }

    [Fact]
    public void read_referential_actions_parses_restrict()
    {
        var fk = new ForeignKey("test_fk");
        fk.ReadReferentialActions("RESTRICT", "RESTRICT");

        fk.OnDelete.ShouldBe(CascadeAction.Restrict);
        fk.OnUpdate.ShouldBe(CascadeAction.Restrict);
    }

    [Fact]
    public void foreign_key_equality_by_name()
    {
        var fk1 = new ForeignKey("fk_test");
        fk1.LinkedTable = new SqliteObjectName("users");
        fk1.LinkColumns("user_id", "id");

        var fk2 = new ForeignKey("fk_test");
        fk2.LinkedTable = new SqliteObjectName("users");
        fk2.LinkColumns("user_id", "id");

        fk1.Equals(fk2).ShouldBeTrue();
    }

    [Fact]
    public void foreign_keys_with_different_tables_are_not_equal()
    {
        var fk1 = new ForeignKey("fk_test");
        fk1.LinkedTable = new SqliteObjectName("users");
        fk1.LinkColumns("user_id", "id");

        var fk2 = new ForeignKey("fk_test");
        fk2.LinkedTable = new SqliteObjectName("accounts");
        fk2.LinkColumns("user_id", "id");

        fk1.Equals(fk2).ShouldBeFalse();
    }

    [Fact]
    public void add_foreign_key_statement_throws_not_supported()
    {
        var fk = new ForeignKey("fk_test");
        var table = new Table("test");
        var writer = new StringWriter();

        Should.Throw<NotSupportedException>(() => fk.WriteAddStatement(table, writer));
    }

    [Fact]
    public void drop_foreign_key_statement_throws_not_supported()
    {
        var fk = new ForeignKey("fk_test");
        var table = new Table("test");
        var writer = new StringWriter();

        Should.Throw<NotSupportedException>(() => fk.WriteDropStatement(table, writer));
    }
}
