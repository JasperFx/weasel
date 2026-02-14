using Shouldly;
using Weasel.Core;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

public class TableDeltaTests
{
    [Fact]
    public void no_difference_when_tables_match()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    [Fact]
    public void create_when_table_does_not_exist()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();

        var delta = new TableDelta(expected, null);

        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public void update_when_column_added()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");
        expected.AddColumn<string>("email");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns.Missing.Count.ShouldBe(1);
        delta.Columns.Missing[0].Name.ShouldBe("email");
    }

    [Fact]
    public void update_when_column_removed()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");
        actual.AddColumn<string>("email");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Columns.Extras.Count.ShouldBe(1);
        delta.Columns.Extras[0].Name.ShouldBe("email");
    }

    [Fact]
    public void invalid_when_column_type_changed()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("age");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<int>("age");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.RequiresTableRecreation.ShouldBeTrue();
        delta.Columns.Different.Count.ShouldBe(1);
    }

    [Fact]
    public void update_when_index_added()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var index = new IndexDefinition("idx_users_email");
        index.AgainstColumns("email");
        expected.Indexes.Add(index);

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email");

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Indexes.Missing.Count.ShouldBe(1);
    }

    [Fact]
    public void update_when_index_removed()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email");

        var index = new IndexDefinition("idx_users_email");
        index.AgainstColumns("email");
        actual.Indexes.Add(index);

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.Indexes.Extras.Count.ShouldBe(1);
    }

    [Fact]
    public void invalid_when_foreign_key_added()
    {
        var expected = new Table("posts");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<int>("user_id");

        var fk = new ForeignKey("fk_posts_user");
        fk.ColumnNames = new[] { "user_id" };
        fk.LinkedTable = new SqliteObjectName("users");
        fk.LinkedNames = new[] { "id" };
        expected.ForeignKeys.Add(fk);

        var actual = new Table("posts");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<int>("user_id");

        var delta = new TableDelta(expected, actual);

        // Foreign key changes require table recreation in SQLite
        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.RequiresTableRecreation.ShouldBeTrue();
    }

    [Fact]
    public void invalid_when_primary_key_changed()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var actual = new Table("users");
        actual.AddColumn<int>("id");
        actual.AddColumn<string>("email").AsPrimaryKey();

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
        delta.RequiresTableRecreation.ShouldBeTrue();
        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public void write_update_for_simple_column_add()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");
        expected.AddColumn<string>("email");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();

        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("ALTER TABLE");
        ddl.ShouldContain("ADD COLUMN");
        ddl.ShouldContain("email");
    }

    [Fact]
    public void write_update_for_table_recreation()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("age");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<int>("age");

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();

        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("Table recreation required");
        ddl.ShouldContain("users_new");
        ddl.ShouldContain("INSERT INTO");
        ddl.ShouldContain("DROP TABLE");
        ddl.ShouldContain("ALTER TABLE");
        ddl.ShouldContain("RENAME TO");
    }

    [Fact]
    public void write_create_for_new_table()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        var delta = new TableDelta(expected, null);
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();

        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("CREATE TABLE");
        ddl.ShouldContain("users");
        ddl.ShouldContain("id");
        ddl.ShouldContain("name");
    }

    [Fact]
    public void has_changes_returns_false_when_tables_match()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);

        delta.HasChanges().ShouldBeFalse();
    }

    [Fact]
    public void has_changes_returns_true_when_column_added()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");
        expected.AddColumn<string>("email");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);

        delta.HasChanges().ShouldBeTrue();
    }

    [Fact]
    public void has_changes_returns_true_when_index_added()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");
        var index = new IndexDefinition("idx_email");
        index.AgainstColumns("email");
        expected.Indexes.Add(index);

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email");

        var delta = new TableDelta(expected, actual);

        delta.HasChanges().ShouldBeTrue();
    }

    [Fact]
    public void has_changes_returns_true_when_primary_key_changed()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var actual = new Table("users");
        actual.AddColumn<int>("id");
        actual.AddColumn<string>("email").AsPrimaryKey();

        var delta = new TableDelta(expected, actual);

        delta.HasChanges().ShouldBeTrue();
    }

    [Fact]
    public void write_rollback_for_new_table()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        var delta = new TableDelta(expected, null);
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();

        delta.WriteRollback(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("DROP TABLE");
        ddl.ShouldContain("users");
    }

    [Fact]
    public void write_rollback_for_simple_column_add()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");
        expected.AddColumn<string>("email");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();

        delta.WriteRollback(migrator, writer);
        var ddl = writer.ToString();

        // Rollback of adding email column should drop it
        ddl.ShouldContain("DROP COLUMN");
        ddl.ShouldContain("email");
    }

    [Fact]
    public void write_rollback_for_index_add()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("email");

        var index = new IndexDefinition("idx_users_email");
        index.AgainstColumns("email");
        expected.Indexes.Add(index);

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("email");

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();

        delta.WriteRollback(migrator, writer);
        var ddl = writer.ToString();

        // Rollback of adding index should drop it
        ddl.ShouldContain("DROP INDEX");
        ddl.ShouldContain("idx_users_email");
    }

    [Fact]
    public void write_rollback_for_table_recreation()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("age");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<int>("age");

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();

        delta.WriteRollback(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("Rollback");
        ddl.ShouldContain("users_rollback");
        ddl.ShouldContain("INSERT INTO");
        ddl.ShouldContain("DROP TABLE");
        ddl.ShouldContain("ALTER TABLE");
        ddl.ShouldContain("RENAME TO");
    }

    // ---- Rename column detection tests ----

    [Fact]
    public void detect_rename_when_column_name_changes_with_same_type()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("full_name"); // renamed from "name"

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name"); // old name

        var delta = new TableDelta(expected, actual);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
        delta.RenamedColumns.Count.ShouldBe(1);
        delta.RenamedColumns[0].Expected.Name.ShouldBe("full_name");
        delta.RenamedColumns[0].Actual.Name.ShouldBe("name");
        delta.RequiresTableRecreation.ShouldBeFalse();
    }

    [Fact]
    public void no_rename_when_types_differ()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<int>("user_age"); // different name AND type

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name"); // different type

        var delta = new TableDelta(expected, actual);

        delta.RenamedColumns.Count.ShouldBe(0);
    }

    [Fact]
    public void no_rename_when_ambiguous_multiple_candidates()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("first_name");
        expected.AddColumn<string>("last_name");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name_a");
        actual.AddColumn<string>("name_b");

        var delta = new TableDelta(expected, actual);

        // Two TEXT extras and two TEXT missing — ambiguous, no renames detected
        delta.RenamedColumns.Count.ShouldBe(0);
    }

    [Fact]
    public void write_update_for_rename_emits_rename_column()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("full_name");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();

        delta.WriteUpdate(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("RENAME COLUMN");
        ddl.ShouldContain("name TO full_name");
        ddl.ShouldNotContain("ADD COLUMN");
        ddl.ShouldNotContain("DROP COLUMN");
    }

    [Fact]
    public void write_rollback_for_rename_reverses_rename()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("full_name");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);
        var writer = new StringWriter();
        var migrator = new SqliteMigrator();

        delta.WriteRollback(migrator, writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("RENAME COLUMN");
        ddl.ShouldContain("full_name TO name");
    }

    [Fact]
    public void has_changes_returns_true_for_rename()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("full_name");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);

        delta.HasChanges().ShouldBeTrue();
    }

    [Fact]
    public void rename_with_add_and_drop_of_different_types()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("full_name"); // rename from "name" (TEXT→TEXT, unambiguous)
        expected.AddColumn<int>("age");          // new column (INTEGER)

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");        // will be renamed
        actual.AddColumn<double>("score");       // will be dropped (REAL)

        var delta = new TableDelta(expected, actual);

        // "name" (TEXT) → "full_name" (TEXT) is unambiguous (only TEXT↔TEXT pair)
        // "score" (REAL) is extra, "age" (INTEGER) is missing — different types, no rename
        delta.RenamedColumns.Count.ShouldBe(1);
        delta.RenamedColumns[0].Expected.Name.ShouldBe("full_name");
        delta.RenamedColumns[0].Actual.Name.ShouldBe("name");

        var writer = new StringWriter();
        delta.WriteUpdate(new SqliteMigrator(), writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("RENAME COLUMN");
        ddl.ShouldContain("ADD COLUMN");
        ddl.ShouldContain("DROP COLUMN");
    }

    // ---- Dropped column dependency validation tests ----

    [Fact]
    public void recreation_required_when_dropping_column_referenced_by_fk()
    {
        var expected = new Table("posts");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("title");
        // user_id is removed from expected

        var actual = new Table("posts");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<int>("user_id");
        actual.AddColumn<string>("title");

        var fk = new ForeignKey("fk_posts_user");
        fk.ColumnNames = new[] { "user_id" };
        fk.LinkedTable = new SqliteObjectName("users");
        fk.LinkedNames = new[] { "id" };
        actual.ForeignKeys.Add(fk);

        var delta = new TableDelta(expected, actual);

        delta.RequiresTableRecreation.ShouldBeTrue();
        delta.Difference.ShouldBe(SchemaPatchDifference.Invalid);
    }

    [Fact]
    public void recreation_required_when_dropping_primary_key_column()
    {
        var expected = new Table("users");
        expected.AddColumn<string>("name");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");

        var delta = new TableDelta(expected, actual);

        delta.RequiresTableRecreation.ShouldBeTrue();
    }

    [Fact]
    public void no_recreation_when_dropping_unreferenced_column()
    {
        var expected = new Table("users");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<string>("name");

        var actual = new Table("users");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<string>("name");
        actual.AddColumn<string>("old_field"); // unreferenced by FK or PK

        var delta = new TableDelta(expected, actual);

        delta.RequiresTableRecreation.ShouldBeFalse();
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public void table_recreation_copies_renamed_columns()
    {
        // When recreation is forced (e.g. by FK change) AND there are renames,
        // the INSERT should map old column names to new ones
        var expected = new Table("posts");
        expected.AddColumn<int>("id").AsPrimaryKey();
        expected.AddColumn<int>("author_id"); // renamed from user_id
        expected.AddColumn<string>("title");

        var fk = new ForeignKey("fk_posts_author");
        fk.ColumnNames = new[] { "author_id" };
        fk.LinkedTable = new SqliteObjectName("users");
        fk.LinkedNames = new[] { "id" };
        expected.ForeignKeys.Add(fk);

        var actual = new Table("posts");
        actual.AddColumn<int>("id").AsPrimaryKey();
        actual.AddColumn<int>("user_id");
        actual.AddColumn<string>("title");

        var delta = new TableDelta(expected, actual);

        // FK change forces recreation
        delta.RequiresTableRecreation.ShouldBeTrue();
        // But rename should still be detected for data copy mapping
        delta.RenamedColumns.Count.ShouldBe(1);

        var writer = new StringWriter();
        delta.WriteUpdate(new SqliteMigrator(), writer);
        var ddl = writer.ToString();

        // INSERT should select user_id into author_id
        ddl.ShouldContain("user_id");
        ddl.ShouldContain("author_id");
    }
}
