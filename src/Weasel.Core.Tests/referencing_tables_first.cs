using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;
using SqlServerTable = Weasel.SqlServer.Tables.Table;

namespace Weasel.Core.Tests;

/// <summary>
///     weasel#890: the order managed tenant partitions remove a tenant's data in. A table that references
///     another one has to be emptied (or have its partition dropped) first.
/// </summary>
public class referencing_tables_first
{
    private static Table table(string name, params string[] references)
    {
        var table = new Table($"public.{name}");
        table.AddColumn<string>("tenant_id").AsPrimaryKey();
        table.AddColumn<int>("id").AsPrimaryKey();

        foreach (var referenced in references)
        {
            var fk = new ForeignKey($"fk_{name}_{referenced}")
            {
                ColumnNames = ["tenant_id", $"{referenced}_id"],
                LinkedNames = ["tenant_id", "id"],
                LinkedTable = new DbObjectName("public", referenced)
            };
            table.ForeignKeys.Add(fk);
        }

        return table;
    }

    private static string[] sorted(params Table[] tables)
        => ReferencingTablesFirst.Sort(tables).Select(x => x.Identifier.Name).ToArray();

    [Fact]
    public void a_referencing_table_comes_before_the_table_it_references()
    {
        // Creation order: the referenced table first, which is how feature schemas yield them
        sorted(table("customer"), table("orders", "customer"))
            .ShouldBe(["orders", "customer"]);
    }

    [Fact]
    public void a_chain_of_references_is_removed_from_the_end()
    {
        sorted(table("customer"), table("orders", "customer"), table("line_item", "orders"))
            .ShouldBe(["line_item", "orders", "customer"]);
    }

    [Fact]
    public void a_table_referencing_two_tables_comes_before_both()
    {
        sorted(table("customer"), table("product"), table("orders", "customer", "product"))
            .ShouldBe(["orders", "customer", "product"]);
    }

    [Fact]
    public void unrelated_tables_keep_their_order()
    {
        sorted(table("alpha"), table("beta"), table("gamma"))
            .ShouldBe(["alpha", "beta", "gamma"]);

        sorted(table("alpha"), table("customer"), table("beta"), table("orders", "customer"), table("gamma"))
            .ShouldBe(["alpha", "orders", "customer", "beta", "gamma"]);
    }

    [Fact]
    public void a_self_reference_does_not_affect_the_order_and_does_not_throw()
    {
        sorted(table("node", "node"), table("leaf", "node"))
            .ShouldBe(["leaf", "node"]);

        sorted(table("node", "node"))
            .ShouldBe(["node"]);
    }

    [Fact]
    public void a_cycle_is_broken_rather_than_refused()
    {
        // Can't be ordered: both tables still come back exactly once, and the removal fails on the cycle
        // the same way it would have without the sort
        var result = sorted(table("chicken", "egg"), table("egg", "chicken"));
        result.Length.ShouldBe(2);
        result.ShouldBe(["egg", "chicken"], ignoreOrder: true);

        // The tables outside the cycle are still ordered around it
        sorted(table("chicken", "egg"), table("egg", "chicken"), table("farm"), table("barn", "farm"))
            .ShouldBe(["egg", "chicken", "barn", "farm"]);
    }

    [Fact]
    public void a_reference_to_a_table_outside_the_set_is_ignored()
    {
        sorted(table("orders", "elsewhere"), table("customer"))
            .ShouldBe(["orders", "customer"]);
    }

    [Fact]
    public void the_linked_table_matches_regardless_of_case()
    {
        var orders = table("orders");
        orders.ForeignKeys.Add(new ForeignKey("fk_orders_customer")
        {
            ColumnNames = ["tenant_id", "customer_id"],
            LinkedNames = ["tenant_id", "id"],
            LinkedTable = new DbObjectName("PUBLIC", "Customer")
        });

        sorted(table("customer"), orders).ShouldBe(["orders", "customer"]);
    }

    [Fact]
    public void works_for_sql_server_tables()
    {
        var customer = new SqlServerTable("dbo.customer");
        customer.AddColumn<int>("tenant_ordinal").AsPrimaryKey();
        customer.AddColumn<int>("id").AsPrimaryKey();

        var orders = new SqlServerTable("dbo.orders");
        orders.AddColumn<int>("tenant_ordinal").AsPrimaryKey();
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.AddColumn<int>("customer_id");
        var fk = orders.FindOrCreateForeignKey("fk_orders_customer");
        fk.ColumnNames = ["tenant_ordinal", "customer_id"];
        fk.LinkedNames = ["tenant_ordinal", "id"];
        fk.LinkedTable = customer.Identifier;

        ReferencingTablesFirst.Sort([customer, orders])
            .Select(x => x.Identifier.Name)
            .ShouldBe(["orders", "customer"]);
    }
}
