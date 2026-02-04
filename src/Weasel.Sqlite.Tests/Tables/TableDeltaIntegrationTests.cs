using Microsoft.Data.Sqlite;
using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

[Collection("integration")]
public class TableDeltaIntegrationTests
{
    private static async Task<SqliteConnection> OpenConnectionAsync()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        return connection;
    }

    [Fact]
    public async Task can_detect_no_changes_when_table_matches()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");

        await table.CreateAsync(connection);

        // Fetch existing and compare
        var delta = await table.FindDeltaAsync(connection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    [Fact]
    public async Task can_detect_missing_column()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table
        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        await actual.CreateAsync(connection);

        // Expected has an additional column
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");
        expected.AddColumn<string>("email");

        var delta = await expected.FindDeltaAsync(connection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns.Missing.Count.ShouldBe(1);
        delta.Columns.Missing[0].Name.ShouldBe("email");
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    [Fact]
    public async Task can_detect_extra_column()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table with extra column
        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");
        actual.AddColumn<string>("email");

        await actual.CreateAsync(connection);

        // Expected has fewer columns
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        var delta = await expected.FindDeltaAsync(connection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns.Extras.Count.ShouldBe(1);
        delta.Columns.Extras[0].Name.ShouldBe("email");
    }

    [Fact]
    public async Task can_detect_column_type_change()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table
        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<int>("age");

        await actual.CreateAsync(connection);

        // Expected has different column type
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("age");

        var delta = await expected.FindDeltaAsync(connection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.RequiresTableRecreation.ShouldBeTrue();
        delta.Columns.Different.Count.ShouldBe(1);
    }

    [Fact]
    public async Task can_detect_missing_index()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table without index
        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email");

        await actual.CreateAsync(connection);

        // Expected has an index
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");
        var index = new IndexDefinition("idx_users_email");
        index.AgainstColumns("email");
        expected.Indexes.Add(index);

        var delta = await expected.FindDeltaAsync(connection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Indexes.Missing.Count.ShouldBe(1);
        delta.Indexes.Missing[0].Name.ShouldBe("idx_users_email");
    }

    [Fact]
    public async Task can_detect_extra_index()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table with index
        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email");
        var index = new IndexDefinition("idx_users_email");
        index.AgainstColumns("email");
        actual.Indexes.Add(index);

        await actual.CreateAsync(connection);

        // Expected has no index
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var delta = await expected.FindDeltaAsync(connection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Indexes.Extras.Count.ShouldBe(1);
    }

    [Fact]
    public async Task can_add_nullable_column_via_migration()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");

        await table.CreateAsync(connection);

        // Add a nullable column
        table.AddColumn<string>("email");

        var delta = await table.FindDeltaAsync(connection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        // Apply the migration
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        await connection.CreateCommand(ddl).ExecuteNonQueryAsync();

        // Verify the column was added
        var afterDelta = await table.FindDeltaAsync(connection);
        afterDelta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task can_add_column_with_default_via_migration()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");

        await table.CreateAsync(connection);

        // Add a column with default
        table.AddColumn<bool>("active").NotNull().DefaultValue("1");

        var delta = await table.FindDeltaAsync(connection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        // Apply the migration
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        await connection.CreateCommand(ddl).ExecuteNonQueryAsync();

        // Verify the column was added
        var afterDelta = await table.FindDeltaAsync(connection);
        afterDelta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task can_drop_column_via_migration()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table with extra column
        var actualTable = new Table("users");
        actualTable.AddColumn<int>("id").AsPrimaryKey();
        actualTable.AddColumn<string>("name");
        actualTable.AddColumn<string>("old_field");

        await actualTable.CreateAsync(connection);

        // Expected table without the column
        var expectedTable = new Table("users");
        expectedTable.AddColumn<int>("id").AsPrimaryKey();
        expectedTable.AddColumn<string>("name");

        var delta = await expectedTable.FindDeltaAsync(connection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns.Extras.Count.ShouldBe(1);

        // Apply the migration
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        await connection.CreateCommand(ddl).ExecuteNonQueryAsync();

        // Verify the column was dropped
        var afterDelta = await expectedTable.FindDeltaAsync(connection);
        afterDelta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task can_add_index_via_migration()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table without index
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("email");

        await table.CreateAsync(connection);

        // Add index to expected schema
        var index = new IndexDefinition("idx_users_email");
        index.AgainstColumns("email");
        table.Indexes.Add(index);

        var delta = await table.FindDeltaAsync(connection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Indexes.Missing.Count.ShouldBe(1);

        // Apply the migration
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        await connection.CreateCommand(ddl).ExecuteNonQueryAsync();

        // Verify the index was added
        var afterDelta = await table.FindDeltaAsync(connection);
        afterDelta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task can_drop_index_via_migration()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table with index
        var actualTable = new Table("users");
        actualTable.AddColumn<int>("id").AsPrimaryKey();
        actualTable.AddColumn<string>("email");
        var index = new IndexDefinition("idx_users_email");
        index.AgainstColumns("email");
        actualTable.Indexes.Add(index);

        await actualTable.CreateAsync(connection);

        // Expected table without index
        var expectedTable = new Table("users");
        expectedTable.AddColumn<int>("id").AsPrimaryKey();
        expectedTable.AddColumn<string>("email");

        var delta = await expectedTable.FindDeltaAsync(connection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Indexes.Extras.Count.ShouldBe(1);

        // Apply the migration
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        await connection.CreateCommand(ddl).ExecuteNonQueryAsync();

        // Verify the index was dropped
        var afterDelta = await expectedTable.FindDeltaAsync(connection);
        afterDelta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task can_recreate_table_for_column_type_change()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table with data
        var actualTable = new Table("users");
        actualTable.AddColumn<int>("id").AsPrimaryKey();
        actualTable.AddColumn<int>("age");

        await actualTable.CreateAsync(connection);

        // Insert test data
        await connection.CreateCommand("INSERT INTO users (id, age) VALUES (1, 25)").ExecuteNonQueryAsync();
        await connection.CreateCommand("INSERT INTO users (id, age) VALUES (2, 30)").ExecuteNonQueryAsync();

        // Expected table with different column type
        var expectedTable = new Table("users");
        expectedTable.AddColumn<int>("id").AsPrimaryKey();
        expectedTable.AddColumn<string>("age");

        var delta = await expectedTable.FindDeltaAsync(connection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.RequiresTableRecreation.ShouldBeTrue();

        // Apply the migration (table recreation)
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("Table recreation required");
        ddl.ShouldContain("users_new");
        ddl.ShouldContain("INSERT INTO");
        ddl.ShouldContain("DROP TABLE");
        ddl.ShouldContain("RENAME TO");

        await connection.CreateCommand(ddl).ExecuteNonQueryAsync();

        // Verify the table was recreated and data was preserved
        var afterDelta = await expectedTable.FindDeltaAsync(connection);
        afterDelta.Difference.ShouldBe(SchemaPatchDifference.None);

        // Verify data was preserved
        var cmd = connection.CreateCommand("SELECT COUNT(*) FROM users");
        var count = (long)(await cmd.ExecuteScalarAsync() ?? 0L);
        count.ShouldBe(2L);
    }

    [Fact]
    public async Task table_recreation_preserves_indexes()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table with index
        var actualTable = new Table("users");
        actualTable.AddColumn<int>("id").AsPrimaryKey();
        actualTable.AddColumn<int>("age");
        actualTable.AddColumn<string>("email");
        var index = new IndexDefinition("idx_users_email");
        index.AgainstColumns("email");
        actualTable.Indexes.Add(index);

        await actualTable.CreateAsync(connection);

        // Expected table with different column type but same index
        var expectedTable = new Table("users");
        expectedTable.AddColumn<int>("id").AsPrimaryKey();
        expectedTable.AddColumn<string>("age"); // Type changed
        expectedTable.AddColumn<string>("email");
        var expectedIndex = new IndexDefinition("idx_users_email");
        expectedIndex.AgainstColumns("email");
        expectedTable.Indexes.Add(expectedIndex);

        var delta = await expectedTable.FindDeltaAsync(connection);
        delta.RequiresTableRecreation.ShouldBeTrue();

        // Apply the migration
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        await connection.CreateCommand(ddl).ExecuteNonQueryAsync();

        // Verify the index was recreated
        var afterDelta = await expectedTable.FindDeltaAsync(connection);
        afterDelta.Difference.ShouldBe(SchemaPatchDifference.None);
        afterDelta.Indexes.Missing.ShouldBeEmpty();
    }
}
