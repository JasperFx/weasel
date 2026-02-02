using Shouldly;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

public class TableColumnTests
{
    [Fact]
    public void create_column_with_name_and_type()
    {
        var column = new TableColumn("email", "VARCHAR(255)");

        column.Name.ShouldBe("email");
        column.Type.ShouldBe("VARCHAR(255)");
    }

    [Fact]
    public void default_allows_nulls()
    {
        var column = new TableColumn("email", "VARCHAR(255)");
        column.AllowNulls.ShouldBeTrue();
    }

    [Fact]
    public void quoted_name_uses_backticks()
    {
        var column = new TableColumn("email", "VARCHAR(255)");
        column.QuotedName.ShouldBe("`email`");
    }

    [Fact]
    public void raw_type_strips_size()
    {
        var column = new TableColumn("email", "VARCHAR(255)");
        column.RawType().ShouldBe("VARCHAR");
    }

    [Fact]
    public void raw_type_handles_type_without_size()
    {
        var column = new TableColumn("count", "INT");
        column.RawType().ShouldBe("INT");
    }

    [Fact]
    public void to_declaration_not_null()
    {
        var column = new TableColumn("email", "VARCHAR(255)") { AllowNulls = false };
        var declaration = column.ToDeclaration();

        declaration.ShouldContain("`email`");
        declaration.ShouldContain("VARCHAR(255)");
        declaration.ShouldContain("NOT NULL");
    }

    [Fact]
    public void to_declaration_allows_null()
    {
        var column = new TableColumn("email", "VARCHAR(255)") { AllowNulls = true };
        var declaration = column.ToDeclaration();

        declaration.ShouldContain("NULL");
        declaration.ShouldNotContain("NOT NULL");
    }

    [Fact]
    public void to_declaration_auto_increment()
    {
        var column = new TableColumn("id", "INT") { IsAutoNumber = true, AllowNulls = false };
        var declaration = column.ToDeclaration();

        declaration.ShouldContain("AUTO_INCREMENT");
    }

    [Fact]
    public void to_declaration_with_default()
    {
        var column = new TableColumn("status", "VARCHAR(50)")
        {
            DefaultExpression = "'active'"
        };
        var declaration = column.ToDeclaration();

        declaration.ShouldContain("DEFAULT 'active'");
    }

    [Fact]
    public void is_equivalent_same_name_and_type()
    {
        var column1 = new TableColumn("email", "VARCHAR(255)");
        var column2 = new TableColumn("email", "VARCHAR(255)");

        column1.IsEquivalentTo(column2).ShouldBeTrue();
    }

    [Fact]
    public void is_equivalent_different_name()
    {
        var column1 = new TableColumn("email", "VARCHAR(255)");
        var column2 = new TableColumn("name", "VARCHAR(255)");

        column1.IsEquivalentTo(column2).ShouldBeFalse();
    }

    [Fact]
    public void is_equivalent_different_base_type()
    {
        var column1 = new TableColumn("data", "TEXT");
        var column2 = new TableColumn("data", "VARCHAR(255)");

        column1.IsEquivalentTo(column2).ShouldBeFalse();
    }

    [Fact]
    public void is_equivalent_ignores_size_differences()
    {
        var column1 = new TableColumn("email", "VARCHAR(100)");
        var column2 = new TableColumn("email", "VARCHAR(255)");

        // RawType() comparison means they are equivalent
        column1.IsEquivalentTo(column2).ShouldBeTrue();
    }

    [Fact]
    public void is_equivalent_different_nullability()
    {
        var column1 = new TableColumn("email", "VARCHAR(255)") { AllowNulls = true };
        var column2 = new TableColumn("email", "VARCHAR(255)") { AllowNulls = false };

        column1.IsEquivalentTo(column2).ShouldBeFalse();
    }

    [Fact]
    public void is_equivalent_case_insensitive_name()
    {
        var column1 = new TableColumn("Email", "VARCHAR(255)");
        var column2 = new TableColumn("email", "VARCHAR(255)");

        column1.IsEquivalentTo(column2).ShouldBeTrue();
    }

    [Fact]
    public void to_string_returns_name_and_type()
    {
        var column = new TableColumn("email", "VARCHAR(255)");
        column.ToString().ShouldBe("email: VARCHAR(255)");
    }

    [Fact]
    public void equals_uses_is_equivalent()
    {
        var column1 = new TableColumn("email", "VARCHAR(255)");
        var column2 = new TableColumn("email", "VARCHAR(255)");

        column1.Equals(column2).ShouldBeTrue();
    }

    [Fact]
    public void hash_code_ignores_size()
    {
        var column1 = new TableColumn("email", "VARCHAR(100)");
        var column2 = new TableColumn("email", "VARCHAR(255)");

        column1.GetHashCode().ShouldBe(column2.GetHashCode());
    }

    [Fact]
    public void primary_key_forces_not_null()
    {
        var column = new TableColumn("id", "INT")
        {
            IsPrimaryKey = true,
            AllowNulls = true // this should be overridden in declaration
        };

        // Primary key columns are always NOT NULL in declaration
        var declaration = column.Declaration();
        declaration.ShouldContain("NOT NULL");
    }
}
