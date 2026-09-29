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
    public void is_equivalent_catches_a_character_length_difference()
    {
        var column1 = new TableColumn("email", "VARCHAR(100)");
        var column2 = new TableColumn("email", "VARCHAR(255)");

        // This asserted the opposite until JasperFx/wolverine#4246. RawType() still throws the
        // parenthesised part away, but a character length is declared by the model, reported faithfully
        // by the catalog, and load-bearing -- a column narrower than the value fails the insert. Widening
        // a varchar used to be invisible here, so the differ never emitted the MODIFY COLUMN and an
        // existing table kept the narrow column forever.
        column1.IsEquivalentTo(column2).ShouldBeFalse();
    }

    [Fact]
    public void is_equivalent_still_ignores_sizes_that_are_not_character_lengths()
    {
        // The reason sizes are stripped in the first place: MySQL 8 reports a bare INT for a column
        // declared int(11), and a DECIMAL carries a precision and a scale rather than a length.
        new TableColumn("count", "INT(11)")
            .IsEquivalentTo(new TableColumn("count", "INT")).ShouldBeTrue();

        new TableColumn("amount", "DECIMAL(18,2)")
            .IsEquivalentTo(new TableColumn("amount", "DECIMAL(10,4)")).ShouldBeTrue();
    }

    /// <summary>
    ///     The declared spelling on the left, what <c>information_schema.COLUMNS.COLUMN_TYPE</c> reports
    ///     for it on the right -- upper-cased, as the table reader stores it. The display-width forms
    ///     (<c>int(11)</c>, <c>tinyint(4)</c>, <c>bigint(20) unsigned</c>) are MySQL 5.7's, and 8.0
    ///     before 8.0.19.
    /// </summary>
    [Theory]
    [InlineData("BOOLEAN", "TINYINT(1)")]
    [InlineData("BOOL", "TINYINT(1)")]
    [InlineData("boolean", "TINYINT(1)")]
    [InlineData("INTEGER", "INT")]
    [InlineData("INTEGER", "INT(11)")]
    [InlineData("INT4", "INT")]
    [InlineData("INT1", "TINYINT")]
    [InlineData("INT2", "SMALLINT")]
    [InlineData("INT3", "MEDIUMINT")]
    [InlineData("MIDDLEINT", "MEDIUMINT")]
    [InlineData("INT8", "BIGINT")]
    [InlineData("SMALLINT(2)", "SMALLINT")]
    [InlineData("BIGINT(19)", "BIGINT(20)")]
    [InlineData("NUMERIC(13,4)", "DECIMAL(13,4)")]
    [InlineData("NUMERIC", "DECIMAL(10,0)")]
    [InlineData("DEC(10,2)", "DECIMAL(10,2)")]
    [InlineData("FIXED(10,2)", "DECIMAL(10,2)")]
    [InlineData("DOUBLE PRECISION", "DOUBLE")]
    [InlineData("REAL", "DOUBLE")]
    [InlineData("FLOAT8", "DOUBLE")]
    [InlineData("FLOAT4", "FLOAT")]
    [InlineData("FLOAT(53)", "DOUBLE")]
    [InlineData("FLOAT(10)", "FLOAT")]
    [InlineData("INT UNSIGNED", "INT UNSIGNED")]
    [InlineData("INT(10) UNSIGNED", "INT UNSIGNED")]
    [InlineData("INT UNSIGNED", "INT(10) UNSIGNED")]
    [InlineData("INT SIGNED", "INT")]
    [InlineData("INT ZEROFILL", "INT(10) UNSIGNED ZEROFILL")]
    [InlineData("SERIAL", "BIGINT UNSIGNED")]
    [InlineData("SERIAL", "BIGINT(20) UNSIGNED")]
    [InlineData("CHARACTER(10)", "CHAR(10)")]
    [InlineData("CHARACTER VARYING(100)", "VARCHAR(100)")]
    [InlineData("CHAR VARYING(100)", "VARCHAR(100)")]
    [InlineData("NATIONAL CHAR(3)", "CHAR(3)")]
    [InlineData("NATIONAL CHARACTER(3)", "CHAR(3)")]
    [InlineData("NCHAR(3)", "CHAR(3)")]
    [InlineData("NATIONAL VARCHAR(20)", "VARCHAR(20)")]
    [InlineData("NATIONAL CHARACTER VARYING(20)", "VARCHAR(20)")]
    [InlineData("NVARCHAR(20)", "VARCHAR(20)")]
    [InlineData("NCHAR VARCHAR(20)", "VARCHAR(20)")]
    [InlineData("NCHAR VARYING(20)", "VARCHAR(20)")]
    [InlineData("VARCHARACTER(20)", "VARCHAR(20)")]
    [InlineData("LONG VARCHAR", "MEDIUMTEXT")]
    [InlineData("LONG", "MEDIUMTEXT")]
    [InlineData("LONG VARBINARY", "MEDIUMBLOB")]
    [InlineData("VARCHAR(20) CHARACTER SET ascii", "VARCHAR(20)")]
    [InlineData("TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", "TEXT")]
    [InlineData("longblob", "LONGBLOB")]
    public void a_synonym_is_equivalent_to_the_type_the_catalog_reports(string declared, string reported)
    {
        new TableColumn("value", declared)
            .IsEquivalentTo(new TableColumn("value", reported)).ShouldBeTrue();

        new TableColumn("value", declared).GetHashCode()
            .ShouldBe(new TableColumn("value", reported).GetHashCode());
    }

    [Theory]
    [InlineData("INTEGER", "BIGINT")]
    [InlineData("INTEGER", "INT UNSIGNED")]
    [InlineData("INT(11)", "INT(10) UNSIGNED")]
    [InlineData("BOOLEAN", "SMALLINT")]
    [InlineData("NUMERIC(13,4)", "DOUBLE")]
    [InlineData("REAL", "FLOAT")]
    [InlineData("FLOAT(10)", "DOUBLE")]
    [InlineData("TEXT", "VARCHAR(255)")]
    [InlineData("LONG VARCHAR", "TEXT")]
    [InlineData("CHARACTER VARYING(200)", "VARCHAR(100)")]
    [InlineData("NATIONAL CHAR(3)", "CHAR(2)")]
    [InlineData("NCHAR VARYING(20)", "CHAR(20)")]
    [InlineData("VARCHARACTER(40)", "VARCHAR(20)")]
    public void a_synonym_does_not_hide_a_real_difference(string declared, string reported)
    {
        new TableColumn("value", declared)
            .IsEquivalentTo(new TableColumn("value", reported)).ShouldBeFalse();
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
