using Shouldly;
using Weasel.Sqlite.Tables;
using Xunit;

namespace Weasel.Sqlite.Tests.Tables;

public class TableColumnTests
{
    [Fact]
    public void can_add_column_with_default_value()
    {
        var table = new Table("users");
        var column = new TableColumn("status", "TEXT")
        {
            DefaultExpression = "'active'"
        };

        var sql = column.AddColumnSql(table);

        sql.ShouldContain("ALTER TABLE");
        sql.ShouldContain("users");
        sql.ShouldContain("ADD COLUMN");
        sql.ShouldContain("status");
        sql.ShouldContain("TEXT");
        sql.ShouldContain("DEFAULT 'active'");
    }

    [Fact]
    public void can_add_nullable_column_without_default()
    {
        var table = new Table("users");
        var column = new TableColumn("middle_name", "TEXT")
        {
            AllowNulls = true
        };

        column.CanAdd().ShouldBeTrue();
    }

    [Fact]
    public void cannot_add_not_null_column_without_default()
    {
        var table = new Table("users");
        var column = new TableColumn("email", "TEXT")
        {
            AllowNulls = false,
            DefaultExpression = null
        };

        column.CanAdd().ShouldBeFalse();
    }

    [Fact]
    public void can_add_not_null_column_with_default()
    {
        var table = new Table("users");
        var column = new TableColumn("active", "INTEGER")
        {
            AllowNulls = false,
            DefaultExpression = "1"
        };

        column.CanAdd().ShouldBeTrue();
    }

    [Fact]
    public void can_drop_column()
    {
        var table = new Table("users");
        var column = new TableColumn("old_field", "TEXT");

        var sql = column.DropColumnSql(table);

        sql.ShouldContain("ALTER TABLE");
        sql.ShouldContain("users");
        sql.ShouldContain("DROP COLUMN");
        sql.ShouldContain("old_field");
    }

    [Fact]
    public void column_with_autoincrement_creates_correct_declaration()
    {
        var table = new Table("users");
        table.AddColumn("id", "INTEGER").AsPrimaryKey().AutoIncrement();
        var column = table.Columns.First();

        var declaration = column.Declaration();

        declaration.ShouldContain("PRIMARY KEY AUTOINCREMENT");
    }

    [Fact]
    public void column_with_generated_virtual_creates_correct_declaration()
    {
        var column = new TableColumn("full_name", "TEXT")
        {
            GeneratedExpression = "first_name || ' ' || last_name",
            GeneratedType = GeneratedColumnType.Virtual
        };

        var declaration = column.Declaration();

        declaration.ShouldContain("GENERATED ALWAYS AS");
        declaration.ShouldContain("first_name || ' ' || last_name");
        declaration.ShouldContain("VIRTUAL");
    }

    [Fact]
    public void column_with_generated_stored_creates_correct_declaration()
    {
        var column = new TableColumn("name_length", "INTEGER")
        {
            GeneratedExpression = "length(name)",
            GeneratedType = GeneratedColumnType.Stored
        };

        var declaration = column.Declaration();

        declaration.ShouldContain("GENERATED ALWAYS AS");
        declaration.ShouldContain("length(name)");
        declaration.ShouldContain("STORED");
    }

    [Fact]
    public void cannot_alter_column_type()
    {
        var table = new Table("users");
        var expected = new TableColumn("age", "INTEGER");
        var actual = new TableColumn("age", "TEXT");

        expected.CanAlter(actual).ShouldBeFalse();
    }

    [Fact]
    public void alter_column_type_throws_not_supported()
    {
        var table = new Table("users");
        var expected = new TableColumn("age", "INTEGER");
        var actual = new TableColumn("age", "TEXT");

        Should.Throw<NotSupportedException>(() => expected.AlterColumnTypeSql(table, actual));
    }

    [Fact]
    public void column_equality_by_name_is_case_insensitive()
    {
        var col1 = new TableColumn("UserName", "TEXT");
        var col2 = new TableColumn("username", "TEXT");

        col1.Equals(col2).ShouldBeTrue();
    }

    [Fact]
    public void column_with_different_types_are_not_equal()
    {
        var col1 = new TableColumn("age", "INTEGER");
        var col2 = new TableColumn("age", "TEXT");

        col1.Equals(col2).ShouldBeFalse();
    }

    [Fact]
    public void column_equality_only_checks_name_and_type()
    {
        // Equals() is used for schema comparison - only checks structural properties
        var col1 = new TableColumn("name", "TEXT") { AllowNulls = true };
        var col2 = new TableColumn("name", "TEXT") { AllowNulls = false };

        col1.Equals(col2).ShouldBeTrue(); // Same name and type

        var col3 = new TableColumn("status", "TEXT") { DefaultExpression = "'active'" };
        var col4 = new TableColumn("status", "TEXT") { DefaultExpression = "'pending'" };

        col3.Equals(col4).ShouldBeTrue(); // Same name and type
    }
}
