using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

public class TableDeltaTests
{
    [Fact]
    public void detect_create_when_no_actual_table()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();

        var delta = new TableDelta(expected, null);

        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public void no_difference_when_tables_match()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        var actual = new Table("weasel_testing.people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.HasChanges().ShouldBeFalse();
    }

    [Fact]
    public void detect_missing_column()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");
        expected.AddColumn<string>("email");

        var actual = new Table("weasel_testing.people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns!.Missing.Count.ShouldBe(1);
        delta.Columns.Missing[0].Name.ShouldBe("email");
    }

    [Fact]
    public void detect_extra_column()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        var actual = new Table("weasel_testing.people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");
        actual.AddColumn<string>("email");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns!.Extras.Count.ShouldBe(1);
        delta.Columns.Extras[0].Name.ShouldBe("email");
    }

    [Fact]
    public void detect_different_column_type()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn("name", "TEXT");

        var actual = new Table("weasel_testing.people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn("name", "VARCHAR(100)");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns!.Different.Count.ShouldBe(1);
    }

    [Fact]
    public void detect_missing_index()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email").AddIndex();

        var actual = new Table("weasel_testing.people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Indexes!.Missing.Count.ShouldBe(1);
    }

    [Fact]
    public void detect_extra_index()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var actual = new Table("weasel_testing.people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email").AddIndex();

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Indexes!.Extras.Count.ShouldBe(1);
    }

    [Fact]
    public void detect_missing_foreign_key()
    {
        var expected = new Table("weasel_testing.orders");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<int>("customer_id").ForeignKeyTo(new Table("weasel_testing.customers"), "id");

        var actual = new Table("weasel_testing.orders");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<int>("customer_id");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.ForeignKeys!.Missing.Count.ShouldBe(1);
    }

    [Fact]
    public void detect_extra_foreign_key()
    {
        var expected = new Table("weasel_testing.orders");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<int>("customer_id");

        var actual = new Table("weasel_testing.orders");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<int>("customer_id").ForeignKeyTo(new Table("weasel_testing.customers"), "id");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.ForeignKeys!.Extras.Count.ShouldBe(1);
    }

    [Fact]
    public void detect_primary_key_change()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("tenant_id").AsPrimaryKey();

        var actual = new Table("weasel_testing.people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("tenant_id");

        var delta = new TableDelta(expected, actual);

        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();
    }

    [Fact]
    public void invalid_when_partition_strategy_changes()
    {
        var expected = new Table("weasel_testing.events");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.PartitionByRange("created_at");

        var actual = new Table("weasel_testing.events");
        actual.AddColumn<int>("id").AsPrimaryKey();

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
    }

    [Fact]
    public void write_update_for_missing_column()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var actual = new Table("weasel_testing.people");
        actual.AddColumn<int>("id").AsPrimaryKey();

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        delta.WriteUpdate(new MySqlMigrator(), writer);

        var sql = writer.ToString();
        sql.ShouldContain("ALTER TABLE `weasel_testing`.`people` ADD COLUMN `email`");
    }

    [Fact]
    public void write_update_for_extra_column()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();

        var actual = new Table("weasel_testing.people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email");

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        delta.WriteUpdate(new MySqlMigrator(), writer);

        var sql = writer.ToString();
        sql.ShouldContain("ALTER TABLE `weasel_testing`.`people` DROP COLUMN `email`");
    }

    [Fact]
    public void write_update_for_missing_index()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email").AddIndex();

        var actual = new Table("weasel_testing.people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email");

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        delta.WriteUpdate(new MySqlMigrator(), writer);

        var sql = writer.ToString();
        sql.ShouldContain("CREATE INDEX `idx_people_email`");
    }

    [Fact]
    public void write_update_for_extra_index()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var actual = new Table("weasel_testing.people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email").AddIndex();

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        delta.WriteUpdate(new MySqlMigrator(), writer);

        var sql = writer.ToString();
        sql.ShouldContain("DROP INDEX `idx_people_email`");
    }

    [Fact]
    public void write_update_for_primary_key_change()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("tenant_id").AsPrimaryKey();

        var actual = new Table("weasel_testing.people");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("tenant_id");

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        delta.WriteUpdate(new MySqlMigrator(), writer);

        var sql = writer.ToString();
        sql.ShouldContain("DROP PRIMARY KEY");
        sql.ShouldContain("ADD PRIMARY KEY");
    }

    [Fact]
    public void write_rollback_for_create()
    {
        var expected = new Table("weasel_testing.people");
        expected.AddColumn<int>("id").AsPrimaryKey();

        var delta = new TableDelta(expected, null);
        var writer = new StringWriter();
        delta.WriteRollback(new MySqlMigrator(), writer);

        var sql = writer.ToString();
        sql.ShouldContain("DROP TABLE IF EXISTS `weasel_testing`.`people`");
    }
}
