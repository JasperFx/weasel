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
        delta.HasChanges().ShouldBeTrue();
        delta.Columns.Missing.Count.ShouldBe(1);
        delta.Columns.Missing[0].Name.ShouldBe("email");
        delta.RequiresTableRecreation.ShouldBeFalse();

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
        delta.HasChanges().ShouldBeTrue();
        delta.Columns.Extras.Count.ShouldBe(1);
        delta.Columns.Extras[0].Name.ShouldBe("old_field");

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
        delta.HasChanges().ShouldBeTrue();
        delta.Indexes.Missing.Count.ShouldBe(1);
        delta.Indexes.Missing[0].Name.ShouldBe("idx_users_email");

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
        delta.HasChanges().ShouldBeTrue();
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
        delta.HasChanges().ShouldBeTrue();
        delta.Columns.Different.Count.ShouldBe(1);

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

    [Fact]
    public async Task can_rename_column_via_migration()
    {
        await using var connection = await OpenConnectionAsync();

        // Create initial table with data
        var actualTable = new Table("users");
        actualTable.AddColumn<int>("id").AsPrimaryKey();
        actualTable.AddColumn<string>("name");

        await actualTable.CreateAsync(connection);

        await connection.CreateCommand("INSERT INTO users (id, name) VALUES (1, 'Alice')").ExecuteNonQueryAsync();
        await connection.CreateCommand("INSERT INTO users (id, name) VALUES (2, 'Bob')").ExecuteNonQueryAsync();

        // Expected table with renamed column
        var expectedTable = new Table("users");
        expectedTable.AddColumn<int>("id").AsPrimaryKey();
        expectedTable.AddColumn<string>("full_name");

        var delta = await expectedTable.FindDeltaAsync(connection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.RenamedColumns.Count.ShouldBe(1);

        // Apply the migration
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();
        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("RENAME COLUMN");

        await connection.CreateCommand(ddl).ExecuteNonQueryAsync();

        // Verify schema matches
        var afterDelta = await expectedTable.FindDeltaAsync(connection);
        afterDelta.Difference.ShouldBe(SchemaPatchDifference.None);

        // Verify data preserved under new column name
        var cmd = connection.CreateCommand("SELECT full_name FROM users WHERE id = 1");
        var result = await cmd.ExecuteScalarAsync();
        result.ShouldBe("Alice");
    }

    [Fact]
    public async Task can_rename_column_with_index()
    {
        await using var connection = await OpenConnectionAsync();

        // Create table with index on column that will be renamed
        var actualTable = new Table("users");
        actualTable.AddColumn<int>("id").AsPrimaryKey();
        actualTable.AddColumn<string>("name");
        var oldIndex = new IndexDefinition("idx_users_name");
        oldIndex.AgainstColumns("name");
        actualTable.Indexes.Add(oldIndex);

        await actualTable.CreateAsync(connection);

        // Expected: renamed column with updated index
        var expectedTable = new Table("users");
        expectedTable.AddColumn<int>("id").AsPrimaryKey();
        expectedTable.AddColumn<string>("full_name");
        var newIndex = new IndexDefinition("idx_users_full_name");
        newIndex.AgainstColumns("full_name");
        expectedTable.Indexes.Add(newIndex);

        var delta = await expectedTable.FindDeltaAsync(connection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.RenamedColumns.Count.ShouldBe(1);

        // Apply the migration
        var writer = new StringWriter();
        delta.WriteUpdate(new SqliteMigrator(), writer);
        var ddl = writer.ToString();

        await connection.CreateCommand(ddl).ExecuteNonQueryAsync();

        // Verify schema matches
        var afterDelta = await expectedTable.FindDeltaAsync(connection);
        afterDelta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task can_detect_partial_index_with_function_in_predicate()
    {
        await using var connection = await OpenConnectionAsync();

        // Create table with partial index that has function in WHERE clause
        var table = new Table("users");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");

        await table.CreateAsync(connection);

        // Create partial index with function in predicate directly
        await connection.CreateCommand(
            "CREATE INDEX idx_long_names ON users (\"name\") WHERE length(\"name\") > 10"
        ).ExecuteNonQueryAsync();

        // Expected table with matching partial index
        var expectedTable = new Table("users");
        expectedTable.AddColumn<int>("id").AsPrimaryKey();
        expectedTable.AddColumn<string>("name");
        var expectedIndex = new IndexDefinition("idx_long_names");
        expectedIndex.AgainstColumns("name");
        expectedIndex.Predicate = "length(\"name\") > 10";
        expectedTable.Indexes.Add(expectedIndex);

        var delta = await expectedTable.FindDeltaAsync(connection);

        // The index should be detected from sqlite_master
        delta.Indexes.Missing.ShouldBeEmpty();
        delta.Indexes.Extras.ShouldBeEmpty();
    }

    [Fact]
    public async Task drop_column_with_fk_reference_triggers_recreation()
    {
        await using var connection = await OpenConnectionAsync();

        // Enable foreign keys
        await connection.CreateCommand("PRAGMA foreign_keys = ON").ExecuteNonQueryAsync();

        // Create referenced table
        var usersTable = new Table("users");
        usersTable.AddColumn<int>("id").AsPrimaryKey();
        usersTable.AddColumn<string>("name");
        await usersTable.CreateAsync(connection);

        // Create table with FK that will need to drop the FK column
        var actualPosts = new Table("posts");
        actualPosts.AddColumn<int>("id").AsPrimaryKey();
        actualPosts.AddColumn<int>("user_id");
        actualPosts.AddColumn<string>("title");

        var fk = new ForeignKey("fk_posts_user");
        fk.ColumnNames = new[] { "user_id" };
        fk.LinkedTable = new SqliteObjectName("users");
        fk.LinkedNames = new[] { "id" };
        actualPosts.ForeignKeys.Add(fk);

        await actualPosts.CreateAsync(connection);

        // Expected: remove the FK column
        var expectedPosts = new Table("posts");
        expectedPosts.AddColumn<int>("id").AsPrimaryKey();
        expectedPosts.AddColumn<string>("title");

        var delta = await expectedPosts.FindDeltaAsync(connection);

        // Should require recreation because the dropped column is referenced by FK
        delta.RequiresTableRecreation.ShouldBeTrue();
        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
    }
}
