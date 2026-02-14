using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

public class creating_tables_in_database: IntegrationContext
{
    [Fact]
    public async Task create_simple_table()
    {
        await DropTableAsync("`weasel_testing`.`people`");

        var table = new Table("weasel_testing.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();

        await table.CreateAsync(theConnection);

        var exists = await table.ExistsInDatabaseAsync(theConnection);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task create_table_with_auto_increment()
    {
        await DropTableAsync("`weasel_testing`.`items`");

        var table = new Table("weasel_testing.items");
        table.AddColumn<int>("id").AsPrimaryKey().AutoNumber();
        table.AddColumn<string>("name");

        await table.CreateAsync(theConnection);

        var exists = await table.ExistsInDatabaseAsync(theConnection);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task create_table_with_index()
    {
        await DropTableAsync("`weasel_testing`.`users`");

        var table = new Table("weasel_testing.users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("email").AddIndex(x => x.IsUnique = true);

        await table.CreateAsync(theConnection);

        var exists = await table.ExistsInDatabaseAsync(theConnection);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task create_table_with_fulltext_index()
    {
        await DropTableAsync("`weasel_testing`.`articles`");

        var table = new Table("weasel_testing.articles");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("content", "TEXT").AddFulltextIndex();

        await table.CreateAsync(theConnection);

        var exists = await table.ExistsInDatabaseAsync(theConnection);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task create_table_with_foreign_key()
    {
        await DropTableAsync("`weasel_testing`.`orders`");
        await DropTableAsync("`weasel_testing`.`customers`");

        var customers = new Table("weasel_testing.customers");
        customers.AddColumn<int>("id").AsPrimaryKey();
        customers.AddColumn<string>("name");
        await customers.CreateAsync(theConnection);

        var orders = new Table("weasel_testing.orders");
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.AddColumn<int>("customer_id").ForeignKeyTo(customers, "id");
        await orders.CreateAsync(theConnection);

        var exists = await orders.ExistsInDatabaseAsync(theConnection);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task create_table_with_cascade_delete()
    {
        await DropTableAsync("`weasel_testing`.`line_items`");
        await DropTableAsync("`weasel_testing`.`invoices`");

        var invoices = new Table("weasel_testing.invoices");
        invoices.AddColumn<int>("id").AsPrimaryKey();
        await invoices.CreateAsync(theConnection);

        var lineItems = new Table("weasel_testing.line_items");
        lineItems.AddColumn<int>("id").AsPrimaryKey();
        lineItems.AddColumn<int>("invoice_id")
            .ForeignKeyTo(invoices, "id", onDelete: CascadeAction.Cascade);
        await lineItems.CreateAsync(theConnection);

        var exists = await lineItems.ExistsInDatabaseAsync(theConnection);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task create_table_with_default_value()
    {
        await DropTableAsync("`weasel_testing`.`tasks`");

        var table = new Table("weasel_testing.tasks");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("status").DefaultValueByString("pending");
        table.AddColumn<int>("priority").DefaultValue(0);

        await table.CreateAsync(theConnection);

        var exists = await table.ExistsInDatabaseAsync(theConnection);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task create_table_with_multi_column_primary_key()
    {
        await DropTableAsync("`weasel_testing`.`tenant_users`");

        var table = new Table("weasel_testing.tenant_users");
        table.AddColumn<int>("tenant_id").AsPrimaryKey();
        table.AddColumn<int>("user_id").AsPrimaryKey();
        table.AddColumn<string>("role");

        await table.CreateAsync(theConnection);

        var exists = await table.ExistsInDatabaseAsync(theConnection);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task create_table_with_multi_column_index()
    {
        await DropTableAsync("`weasel_testing`.`products`");

        var table = new Table("weasel_testing.products");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("category");
        table.AddColumn<string>("name");

        table.Indexes.Add(new IndexDefinition("idx_products_category_name")
        {
            Columns = new[] { "category", "name" }
        });

        await table.CreateAsync(theConnection);

        var exists = await table.ExistsInDatabaseAsync(theConnection);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task create_table_with_charset_and_collation()
    {
        await DropTableAsync("`weasel_testing`.`messages`");

        var table = new Table("weasel_testing.messages");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("content");
        table.Charset = "utf8mb4";
        table.Collation = "utf8mb4_unicode_ci";

        await table.CreateAsync(theConnection);

        var exists = await table.ExistsInDatabaseAsync(theConnection);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task drop_then_create_mode()
    {
        await DropTableAsync("`weasel_testing`.`recreate_test`");

        // Create initial table
        var table1 = new Table("weasel_testing.recreate_test");
        table1.AddColumn<int>("id").AsPrimaryKey();
        table1.AddColumn<string>("name");
        await table1.CreateAsync(theConnection);

        // Create with drop mode
        var table2 = new Table("weasel_testing.recreate_test");
        table2.AddColumn<int>("id").AsPrimaryKey();
        table2.AddColumn<string>("different_column");

        var writer = new StringWriter();
        var migrator = new MySqlMigrator { TableCreation = CreationStyle.DropThenCreate };
        table2.WriteCreateStatement(migrator, writer);

        var sql = writer.ToString();
        sql.ShouldContain("DROP TABLE IF EXISTS");
        sql.ShouldContain("CREATE TABLE IF NOT EXISTS");
    }

    [Fact]
    public async Task fetch_existing_table()
    {
        await DropTableAsync("`weasel_testing`.`fetch_test`");

        var table = new Table("weasel_testing.fetch_test");
        table.AddColumn<int>("id").AsPrimaryKey().AutoNumber();
        table.AddColumn<string>("email").AddIndex(x => x.IsUnique = true);
        table.AddColumn<string>("name").NotNull();

        await table.CreateAsync(theConnection);

        var existing = await table.FetchExistingAsync(theConnection);

        existing.ShouldNotBeNull();
        existing.Columns.Count.ShouldBe(3);
        existing.HasColumn("id").ShouldBeTrue();
        existing.HasColumn("email").ShouldBeTrue();
        existing.HasColumn("name").ShouldBeTrue();
        existing.Indexes.Count.ShouldBeGreaterThanOrEqualTo(1);
    }

    [Fact]
    public async Task fetch_existing_returns_null_for_missing_table()
    {
        await DropTableAsync("`weasel_testing`.`nonexistent_table`");

        var table = new Table("weasel_testing.nonexistent_table");
        table.AddColumn<int>("id").AsPrimaryKey();

        var existing = await table.FetchExistingAsync(theConnection);

        existing.ShouldBeNull();
    }
}
