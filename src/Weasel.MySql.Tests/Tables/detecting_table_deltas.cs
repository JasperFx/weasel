using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

public class detecting_table_deltas: IntegrationContext
{
    [Fact]
    public async Task detect_no_changes()
    {
        await DropTableAsync("`weasel_testing`.`delta_test_1`");

        var expected = new Table("weasel_testing.delta_test_1");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        await expected.CreateAsync(theConnection);

        var delta = await expected.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task detect_table_needs_creation()
    {
        await DropTableAsync("`weasel_testing`.`delta_test_2`");

        var expected = new Table("weasel_testing.delta_test_2");
        expected.AddColumn<int>("id").AsPrimaryKey();

        var delta = await expected.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public async Task detect_missing_column()
    {
        await DropTableAsync("`weasel_testing`.`delta_test_3`");

        // Create table with just id
        var actual = new Table("weasel_testing.delta_test_3");
        actual.AddColumn<int>("id").AsPrimaryKey();
        await actual.CreateAsync(theConnection);

        // Expected has additional column
        var expected = new Table("weasel_testing.delta_test_3");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var delta = await expected.FindDeltaAsync(theConnection) as TableDelta;

        delta!.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns!.Missing.Count.ShouldBe(1);
        delta.Columns.Missing[0].Name.ShouldBe("email");
    }

    [Fact]
    public async Task detect_extra_column()
    {
        await DropTableAsync("`weasel_testing`.`delta_test_4`");

        // Create table with extra column
        var actual = new Table("weasel_testing.delta_test_4");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("obsolete_column");
        await actual.CreateAsync(theConnection);

        // Expected doesn't have the column
        var expected = new Table("weasel_testing.delta_test_4");
        expected.AddColumn<int>("id").AsPrimaryKey();

        var delta = await expected.FindDeltaAsync(theConnection) as TableDelta;

        delta!.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns!.Extras.Count.ShouldBe(1);
        delta.Columns.Extras[0].Name.ShouldBe("obsolete_column");
    }

    [Fact]
    public async Task detect_missing_index()
    {
        await DropTableAsync("`weasel_testing`.`delta_test_5`");

        // Create table without index
        var actual = new Table("weasel_testing.delta_test_5");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email");
        await actual.CreateAsync(theConnection);

        // Expected has index
        var expected = new Table("weasel_testing.delta_test_5");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email").AddIndex();

        var delta = await expected.FindDeltaAsync(theConnection) as TableDelta;

        delta!.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Indexes!.Missing.Count.ShouldBe(1);
    }

    [Fact]
    public async Task detect_extra_index()
    {
        await DropTableAsync("`weasel_testing`.`delta_test_6`");

        // Create table with index
        var actual = new Table("weasel_testing.delta_test_6");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email").AddIndex();
        await actual.CreateAsync(theConnection);

        // Expected doesn't have index
        var expected = new Table("weasel_testing.delta_test_6");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var delta = await expected.FindDeltaAsync(theConnection) as TableDelta;

        delta!.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Indexes!.Extras.Count.ShouldBe(1);
    }

    [Fact]
    public async Task detect_missing_foreign_key()
    {
        await DropTableAsync("`weasel_testing`.`delta_orders`");
        await DropTableAsync("`weasel_testing`.`delta_customers`");

        // Create parent table
        var customers = new Table("weasel_testing.delta_customers");
        customers.AddColumn<int>("id").AsPrimaryKey();
        await customers.CreateAsync(theConnection);

        // Create table without FK
        var actualOrders = new Table("weasel_testing.delta_orders");
        actualOrders.AddColumn<int>("id").AsPrimaryKey();
        actualOrders.AddColumn<int>("customer_id");
        await actualOrders.CreateAsync(theConnection);

        // Expected has FK
        var expectedOrders = new Table("weasel_testing.delta_orders");
        expectedOrders.AddColumn<int>("id").AsPrimaryKey();
        expectedOrders.AddColumn<int>("customer_id").ForeignKeyTo(customers, "id");

        var delta = await expectedOrders.FindDeltaAsync(theConnection) as TableDelta;

        delta!.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.ForeignKeys!.Missing.Count.ShouldBe(1);
    }

    [Fact]
    public async Task apply_changes_adds_missing_column()
    {
        await DropTableAsync("`weasel_testing`.`apply_test_1`");

        // Create table without email
        var actual = new Table("weasel_testing.apply_test_1");
        actual.AddColumn<int>("id").AsPrimaryKey();
        await actual.CreateAsync(theConnection);

        // Expected has email
        var expected = new Table("weasel_testing.apply_test_1");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        await expected.ApplyChangesAsync(theConnection);

        var afterDelta = await expected.FindDeltaAsync(theConnection);
        afterDelta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task apply_changes_adds_missing_index()
    {
        await DropTableAsync("`weasel_testing`.`apply_test_2`");

        // Create table without index
        var actual = new Table("weasel_testing.apply_test_2");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email");
        await actual.CreateAsync(theConnection);

        // Expected has index
        var expected = new Table("weasel_testing.apply_test_2");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email").AddIndex();

        await expected.ApplyChangesAsync(theConnection);

        var afterDelta = await expected.FindDeltaAsync(theConnection);
        afterDelta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task apply_changes_creates_table_if_not_exists()
    {
        await DropTableAsync("`weasel_testing`.`apply_test_3`");

        var expected = new Table("weasel_testing.apply_test_3");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        await expected.ApplyChangesAsync(theConnection);

        var exists = await expected.ExistsInDatabaseAsync(theConnection);
        exists.ShouldBeTrue();
    }

    [Fact]
    public async Task fetch_and_compare_auto_increment()
    {
        await DropTableAsync("`weasel_testing`.`auto_inc_test`");

        var expected = new Table("weasel_testing.auto_inc_test");
        expected.AddColumn<int>("id").AsPrimaryKey().AutoNumber();
        expected.AddColumn<string>("name");

        await expected.CreateAsync(theConnection);

        var existing = await expected.FetchExistingAsync(theConnection);

        existing.ShouldNotBeNull();
        existing.ColumnFor("id")!.IsAutoNumber.ShouldBeTrue();
    }

    [Fact]
    public async Task fetch_and_compare_unique_index()
    {
        await DropTableAsync("`weasel_testing`.`unique_idx_test`");

        var expected = new Table("weasel_testing.unique_idx_test");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email").AddIndex(x => x.IsUnique = true);

        await expected.CreateAsync(theConnection);

        var existing = await expected.FetchExistingAsync(theConnection);

        existing.ShouldNotBeNull();
        existing.Indexes.Count.ShouldBeGreaterThanOrEqualTo(1);
        existing.Indexes.Any(i => i.IsUnique).ShouldBeTrue();
    }
}
