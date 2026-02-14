using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests.Tables;

public class ForeignKeyTests
{
    [Fact]
    public void basic_foreign_key_ddl()
    {
        var parent = new Table("WEASEL.ORDERS");
        var fk = new ForeignKey("fk_orders_customer")
        {
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" },
            LinkedTable = DbObjectName.Parse(OracleProvider.Instance, "WEASEL.CUSTOMERS")
        };

        var ddl = fk.ToDDL(parent);
        ddl.ShouldContain("ALTER TABLE WEASEL.ORDERS");
        ddl.ShouldContain("ADD CONSTRAINT fk_orders_customer FOREIGN KEY(customer_id)");
        ddl.ShouldContain("REFERENCES WEASEL.CUSTOMERS(id)");
    }

    [Fact]
    public void foreign_key_with_cascade_delete()
    {
        var parent = new Table("WEASEL.ORDERS");
        var fk = new ForeignKey("fk_orders_customer")
        {
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" },
            LinkedTable = DbObjectName.Parse(OracleProvider.Instance, "WEASEL.CUSTOMERS"),
            OnDelete = CascadeAction.Cascade
        };

        var ddl = fk.ToDDL(parent);
        ddl.ShouldContain("ON DELETE CASCADE");
    }

    [Fact]
    public void foreign_key_with_set_null_delete()
    {
        var parent = new Table("WEASEL.ORDERS");
        var fk = new ForeignKey("fk_orders_customer")
        {
            ColumnNames = new[] { "customer_id" },
            LinkedNames = new[] { "id" },
            LinkedTable = DbObjectName.Parse(OracleProvider.Instance, "WEASEL.CUSTOMERS"),
            OnDelete = CascadeAction.SetNull
        };

        var ddl = fk.ToDDL(parent);
        ddl.ShouldContain("ON DELETE SET NULL");
    }

    [Fact]
    public void multi_column_foreign_key()
    {
        var parent = new Table("WEASEL.ORDER_ITEMS");
        var fk = new ForeignKey("fk_items_order")
        {
            ColumnNames = new[] { "order_id", "tenant_id" },
            LinkedNames = new[] { "id", "tenant_id" },
            LinkedTable = DbObjectName.Parse(OracleProvider.Instance, "WEASEL.ORDERS")
        };

        var ddl = fk.ToDDL(parent);
        ddl.ShouldContain("FOREIGN KEY(order_id, tenant_id)");
        ddl.ShouldContain("REFERENCES WEASEL.ORDERS(id, tenant_id)");
    }

    [Fact]
    public void link_columns()
    {
        var fk = new ForeignKey("fk_test");
        fk.LinkColumns("col1", "ref1");
        fk.LinkColumns("col2", "ref2");

        fk.ColumnNames.ShouldBe(new[] { "col1", "col2" });
        fk.LinkedNames.ShouldBe(new[] { "ref1", "ref2" });
    }

    [Fact]
    public void read_referential_actions()
    {
        var fk = new ForeignKey("fk_test");
        fk.ReadReferentialActions("CASCADE");
        fk.OnDelete.ShouldBe(CascadeAction.Cascade);
    }

    [Fact]
    public void drop_statement()
    {
        var parent = new Table("WEASEL.ORDERS");
        var fk = new ForeignKey("fk_orders_customer");

        var writer = new StringWriter();
        fk.WriteDropStatement(parent, writer);

        writer.ToString().ShouldContain("ALTER TABLE WEASEL.ORDERS DROP CONSTRAINT fk_orders_customer");
    }

    [Fact]
    public void equality_same_foreign_keys()
    {
        var fk1 = new ForeignKey("fk_test")
        {
            ColumnNames = new[] { "col1" },
            LinkedNames = new[] { "ref1" },
            LinkedTable = DbObjectName.Parse(OracleProvider.Instance, "WEASEL.REF_TABLE")
        };

        var fk2 = new ForeignKey("fk_test")
        {
            ColumnNames = new[] { "col1" },
            LinkedNames = new[] { "ref1" },
            LinkedTable = DbObjectName.Parse(OracleProvider.Instance, "WEASEL.REF_TABLE")
        };

        fk1.Equals(fk2).ShouldBeTrue();
    }

    [Fact]
    public void equality_different_cascade_action()
    {
        var fk1 = new ForeignKey("fk_test")
        {
            ColumnNames = new[] { "col1" },
            LinkedNames = new[] { "ref1" },
            LinkedTable = DbObjectName.Parse(OracleProvider.Instance, "WEASEL.REF_TABLE"),
            OnDelete = CascadeAction.NoAction
        };

        var fk2 = new ForeignKey("fk_test")
        {
            ColumnNames = new[] { "col1" },
            LinkedNames = new[] { "ref1" },
            LinkedTable = DbObjectName.Parse(OracleProvider.Instance, "WEASEL.REF_TABLE"),
            OnDelete = CascadeAction.Cascade
        };

        fk1.Equals(fk2).ShouldBeFalse();
    }
}
