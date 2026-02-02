using Shouldly;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

public class ForeignKeyTests
{
    [Fact]
    public void create_with_name()
    {
        var fk = new ForeignKey("fk_orders_customer");
        fk.Name.ShouldBe("fk_orders_customer");
    }

    [Fact]
    public void default_cascade_actions_are_no_action()
    {
        var fk = new ForeignKey("fk_test");
        fk.OnDelete.ShouldBe(CascadeAction.NoAction);
        fk.OnUpdate.ShouldBe(CascadeAction.NoAction);
    }

    [Fact]
    public void set_column_names()
    {
        var fk = new ForeignKey("fk_test")
        {
            ColumnNames = new[] { "customer_id" }
        };

        fk.ColumnNames.ShouldBe(new[] { "customer_id" });
    }

    [Fact]
    public void set_linked_names()
    {
        var fk = new ForeignKey("fk_test")
        {
            LinkedNames = new[] { "id" }
        };

        fk.LinkedNames.ShouldBe(new[] { "id" });
    }

    [Fact]
    public void link_columns()
    {
        var fk = new ForeignKey("fk_test");
        fk.LinkColumns("customer_id", "id");

        fk.ColumnNames.ShouldBe(new[] { "customer_id" });
        fk.LinkedNames.ShouldBe(new[] { "id" });
    }

    [Fact]
    public void link_multiple_columns()
    {
        var fk = new ForeignKey("fk_test");
        fk.LinkColumns("tenant_id", "id");
        fk.LinkColumns("customer_id", "customer_id");

        fk.ColumnNames.ShouldBe(new[] { "tenant_id", "customer_id" });
        fk.LinkedNames.ShouldBe(new[] { "id", "customer_id" });
    }

    [Fact]
    public void to_ddl_basic()
    {
        var orders = new Table("weasel_testing.orders");
        var customers = new Table("weasel_testing.customers");

        var fk = new ForeignKey("fk_orders_customer")
        {
            LinkedTable = customers.Identifier,
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" }
        };

        var ddl = fk.ToDDL(orders);

        ddl.ShouldContain("ALTER TABLE `weasel_testing`.`orders`");
        ddl.ShouldContain("ADD CONSTRAINT `fk_orders_customer`");
        ddl.ShouldContain("FOREIGN KEY (`customer_id`)");
        ddl.ShouldContain("REFERENCES `weasel_testing`.`customers` (`id`)");
    }

    [Fact]
    public void to_ddl_with_cascade_delete()
    {
        var orders = new Table("weasel_testing.orders");
        var customers = new Table("weasel_testing.customers");

        var fk = new ForeignKey("fk_orders_customer")
        {
            LinkedTable = customers.Identifier,
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" },
            OnDelete = CascadeAction.Cascade
        };

        var ddl = fk.ToDDL(orders);

        ddl.ShouldContain("ON DELETE CASCADE");
    }

    [Fact]
    public void to_ddl_with_cascade_update()
    {
        var orders = new Table("weasel_testing.orders");
        var customers = new Table("weasel_testing.customers");

        var fk = new ForeignKey("fk_orders_customer")
        {
            LinkedTable = customers.Identifier,
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" },
            OnUpdate = CascadeAction.Cascade
        };

        var ddl = fk.ToDDL(orders);

        ddl.ShouldContain("ON UPDATE CASCADE");
    }

    [Fact]
    public void to_ddl_with_set_null()
    {
        var orders = new Table("weasel_testing.orders");
        var customers = new Table("weasel_testing.customers");

        var fk = new ForeignKey("fk_orders_customer")
        {
            LinkedTable = customers.Identifier,
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" },
            OnDelete = CascadeAction.SetNull
        };

        var ddl = fk.ToDDL(orders);

        ddl.ShouldContain("ON DELETE SET NULL");
    }

    [Fact]
    public void to_ddl_with_restrict()
    {
        var orders = new Table("weasel_testing.orders");
        var customers = new Table("weasel_testing.customers");

        var fk = new ForeignKey("fk_orders_customer")
        {
            LinkedTable = customers.Identifier,
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" },
            OnDelete = CascadeAction.Restrict
        };

        var ddl = fk.ToDDL(orders);

        ddl.ShouldContain("ON DELETE RESTRICT");
    }

    [Fact]
    public void to_ddl_multi_column()
    {
        var orders = new Table("weasel_testing.orders");
        var customers = new Table("weasel_testing.customers");

        var fk = new ForeignKey("fk_orders_customer")
        {
            LinkedTable = customers.Identifier,
            ColumnNames = new[] { "tenant_id", "customer_id" },
            LinkedNames = new[] { "tenant_id", "id" }
        };

        var ddl = fk.ToDDL(orders);

        ddl.ShouldContain("FOREIGN KEY (`tenant_id`, `customer_id`)");
        ddl.ShouldContain("REFERENCES `weasel_testing`.`customers` (`tenant_id`, `id`)");
    }

    [Fact]
    public void throws_if_linked_table_not_set()
    {
        var orders = new Table("weasel_testing.orders");

        var fk = new ForeignKey("fk_test")
        {
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" }
        };

        Should.Throw<InvalidOperationException>(() => fk.ToDDL(orders));
    }

    [Fact]
    public void read_referential_actions()
    {
        var fk = new ForeignKey("fk_test");
        fk.ReadReferentialActions("CASCADE", "SET NULL");

        fk.OnDelete.ShouldBe(CascadeAction.Cascade);
        fk.OnUpdate.ShouldBe(CascadeAction.SetNull);
    }

    [Fact]
    public void is_equivalent_same_fk()
    {
        var fk1 = new ForeignKey("fk_test")
        {
            LinkedTable = new MySqlObjectName("db", "customers"),
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" },
            OnDelete = CascadeAction.Cascade
        };

        var fk2 = new ForeignKey("fk_test")
        {
            LinkedTable = new MySqlObjectName("db", "customers"),
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" },
            OnDelete = CascadeAction.Cascade
        };

        fk1.IsEquivalentTo(fk2).ShouldBeTrue();
    }

    [Fact]
    public void is_not_equivalent_different_name()
    {
        var fk1 = new ForeignKey("fk_test1")
        {
            LinkedTable = new MySqlObjectName("db", "customers"),
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" }
        };

        var fk2 = new ForeignKey("fk_test2")
        {
            LinkedTable = new MySqlObjectName("db", "customers"),
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" }
        };

        fk1.IsEquivalentTo(fk2).ShouldBeFalse();
    }

    [Fact]
    public void is_not_equivalent_different_linked_table()
    {
        var fk1 = new ForeignKey("fk_test")
        {
            LinkedTable = new MySqlObjectName("db", "customers"),
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" }
        };

        var fk2 = new ForeignKey("fk_test")
        {
            LinkedTable = new MySqlObjectName("db", "users"),
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" }
        };

        fk1.IsEquivalentTo(fk2).ShouldBeFalse();
    }

    [Fact]
    public void is_not_equivalent_different_cascade_action()
    {
        var fk1 = new ForeignKey("fk_test")
        {
            LinkedTable = new MySqlObjectName("db", "customers"),
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" },
            OnDelete = CascadeAction.Cascade
        };

        var fk2 = new ForeignKey("fk_test")
        {
            LinkedTable = new MySqlObjectName("db", "customers"),
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" },
            OnDelete = CascadeAction.SetNull
        };

        fk1.IsEquivalentTo(fk2).ShouldBeFalse();
    }
}
