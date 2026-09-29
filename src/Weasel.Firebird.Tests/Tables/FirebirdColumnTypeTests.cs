using Shouldly;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests.Tables;

public class FirebirdColumnTypeTests
{
    private static readonly FirebirdServerVersion Firebird3 = new(3, 0, 14);
    private static readonly FirebirdServerVersion Firebird5 = new(5, 0, 4);

    [Theory]
    [InlineData("INT", "INTEGER")]
    [InlineData("integer", "INTEGER")]
    [InlineData("SMALLINT", "SMALLINT")]
    [InlineData("BIGINT", "BIGINT")]
    [InlineData("INT128", "INT128")]
    [InlineData("DEC(10,2)", "DECIMAL(10,2)")]
    [InlineData("DECIMAL(18, 4)", "DECIMAL(18,4)")]
    [InlineData("NUMERIC(18,4)", "NUMERIC(18,4)")]
    [InlineData("NUMERIC(9)", "NUMERIC(9,0)")]
    [InlineData("NUMERIC", "NUMERIC")]
    [InlineData("REAL", "FLOAT")]
    [InlineData("FLOAT", "FLOAT")]
    [InlineData("DOUBLE PRECISION", "DOUBLE PRECISION")]
    [InlineData("double   precision", "DOUBLE PRECISION")]
    [InlineData("DECFLOAT", "DECFLOAT(34)")]
    [InlineData("DECFLOAT(16)", "DECFLOAT(16)")]
    [InlineData("BOOLEAN", "BOOLEAN")]
    [InlineData("DATE", "DATE")]
    [InlineData("TIME WITHOUT TIME ZONE", "TIME")]
    [InlineData("TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP")]
    [InlineData("TIME WITH TIME ZONE", "TIME WITH TIME ZONE")]
    [InlineData("timestamp with time zone", "TIMESTAMP WITH TIME ZONE")]
    [InlineData("CHAR", "CHAR(1)")]
    [InlineData("CHARACTER(5)", "CHAR(5)")]
    [InlineData("VARCHAR(10)", "VARCHAR(10)")]
    [InlineData("varchar ( 10 )", "VARCHAR(10)")]
    [InlineData("CHARACTER VARYING(10)", "VARCHAR(10)")]
    [InlineData("CHAR VARYING(10)", "VARCHAR(10)")]
    [InlineData("NCHAR(3)", "CHAR(3) CHARACTER SET ISO8859_1")]
    [InlineData("NATIONAL CHARACTER(3)", "CHAR(3) CHARACTER SET ISO8859_1")]
    [InlineData("NATIONAL CHARACTER VARYING(4)", "VARCHAR(4) CHARACTER SET ISO8859_1")]
    [InlineData("NCHAR VARYING(4)", "VARCHAR(4) CHARACTER SET ISO8859_1")]
    [InlineData("BINARY(16)", "CHAR(16) CHARACTER SET OCTETS")]
    [InlineData("VARBINARY(8)", "VARCHAR(8) CHARACTER SET OCTETS")]
    [InlineData("BINARY VARYING(8)", "VARCHAR(8) CHARACTER SET OCTETS")]
    [InlineData("CHAR(16) CHARACTER SET OCTETS", "CHAR(16) CHARACTER SET OCTETS")]
    [InlineData("varchar(10) character set utf8 collate unicode_ci", "VARCHAR(10) CHARACTER SET UTF8 COLLATE UNICODE_CI")]
    [InlineData("VARCHAR(10) COLLATE UNICODE_CI", "VARCHAR(10) COLLATE UNICODE_CI")]
    [InlineData("BLOB", "BLOB SUB_TYPE BINARY")]
    [InlineData("BLOB SUB_TYPE 0", "BLOB SUB_TYPE BINARY")]
    [InlineData("BLOB SUB_TYPE 1", "BLOB SUB_TYPE TEXT")]
    [InlineData("BLOB SUB_TYPE TEXT", "BLOB SUB_TYPE TEXT")]
    [InlineData("BLOB SUB_TYPE TEXT SEGMENT SIZE 80", "BLOB SUB_TYPE TEXT")]
    [InlineData("BLOB SEGMENT SIZE 80 SUB_TYPE TEXT", "BLOB SUB_TYPE TEXT")]
    [InlineData("BLOB(80, 1)", "BLOB SUB_TYPE TEXT")]
    [InlineData("BLOB(80)", "BLOB SUB_TYPE BINARY")]
    [InlineData("BLOB SUB_TYPE TEXT CHARACTER SET UTF8", "BLOB SUB_TYPE TEXT CHARACTER SET UTF8")]
    public void folds_a_declared_type_onto_the_catalog_spelling(string declared, string expected)
    {
        FirebirdColumnType.Parse(declared).ToString().ShouldBe(expected);
    }

    /// <summary>
    ///     Firebird 3 reads FLOAT(p) as decimal digits and stores a double from 8; Firebird 4 and 5
    ///     read binary digits and switch at 25. Measured on 3.0.14, 4.0.7 and 5.0.4.
    /// </summary>
    [Theory]
    [InlineData("FLOAT(7)", 3, "FLOAT")]
    [InlineData("FLOAT(8)", 3, "DOUBLE PRECISION")]
    [InlineData("FLOAT(10)", 3, "DOUBLE PRECISION")]
    [InlineData("FLOAT(10)", 4, "FLOAT")]
    [InlineData("FLOAT(24)", 5, "FLOAT")]
    [InlineData("FLOAT(25)", 5, "DOUBLE PRECISION")]
    public void float_with_a_precision_depends_on_the_server(string declared, int major, string expected)
    {
        FirebirdColumnType.Parse(declared, new FirebirdServerVersion(major, 0, 0)).ToString().ShouldBe(expected);
    }

    [Fact]
    public void float_with_a_precision_and_no_known_server_follows_firebird_4()
    {
        FirebirdColumnType.Parse("FLOAT(10)").ToString().ShouldBe("FLOAT");
        FirebirdColumnType.Parse("FLOAT(30)").ToString().ShouldBe("DOUBLE PRECISION");
    }

    /// <summary>
    ///     The catalog cheat sheet from the Firebird 3/4/5 spike: RDB$FIELD_TYPE, sub-type, precision,
    ///     scale and character length rebuilt into the spelling a model writes.
    /// </summary>
    [Theory]
    [InlineData(7, 0, null, 0, null, null, "SMALLINT")]
    [InlineData(8, 0, 0, 0, null, null, "INTEGER")]
    [InlineData(16, 0, 0, 0, null, null, "BIGINT")]
    [InlineData(26, 0, 0, 0, null, null, "INT128")]
    [InlineData(8, 1, 9, 0, null, null, "NUMERIC(9,0)")]
    [InlineData(16, 1, 18, -4, null, null, "NUMERIC(18,4)")]
    [InlineData(8, 2, 4, -2, null, null, "DECIMAL(4,2)")]
    [InlineData(7, 1, 4, -1, null, null, "NUMERIC(4,1)")]
    [InlineData(26, 1, 38, -4, null, null, "NUMERIC(38,4)")]
    [InlineData(10, null, null, null, null, null, "FLOAT")]
    [InlineData(27, null, null, null, null, null, "DOUBLE PRECISION")]
    [InlineData(24, null, null, null, null, null, "DECFLOAT(16)")]
    [InlineData(25, null, null, null, null, null, "DECFLOAT(34)")]
    [InlineData(23, null, null, null, null, null, "BOOLEAN")]
    [InlineData(12, null, null, null, null, null, "DATE")]
    [InlineData(13, null, null, null, null, null, "TIME")]
    [InlineData(35, null, null, null, null, null, "TIMESTAMP")]
    [InlineData(28, null, null, null, null, null, "TIME WITH TIME ZONE")]
    [InlineData(29, null, null, null, null, null, "TIMESTAMP WITH TIME ZONE")]
    [InlineData(14, 0, null, null, 10, "UTF8", "CHAR(10) CHARACTER SET UTF8")]
    [InlineData(14, 0, null, null, 16, "OCTETS", "CHAR(16) CHARACTER SET OCTETS")]
    [InlineData(14, 1, null, null, 16, "OCTETS", "CHAR(16) CHARACTER SET OCTETS")]
    [InlineData(37, 0, null, null, 120, "UTF8", "VARCHAR(120) CHARACTER SET UTF8")]
    [InlineData(37, 0, null, null, 120, "NONE", "VARCHAR(120) CHARACTER SET NONE")]
    [InlineData(37, 1, null, null, 8, "OCTETS", "VARCHAR(8) CHARACTER SET OCTETS")]
    [InlineData(261, 0, null, null, null, null, "BLOB SUB_TYPE BINARY")]
    [InlineData(261, 1, null, null, null, "UTF8", "BLOB SUB_TYPE TEXT CHARACTER SET UTF8")]
    [InlineData(261, 6, null, null, null, null, "BLOB SUB_TYPE 6")]
    public void rebuilds_the_type_the_catalog_describes(int fieldType, int? subType, int? precision, int? scale,
        int? characterLength, string? characterSet, string expected)
    {
        FirebirdColumnType.FromCatalog(fieldType, subType, precision, scale, characterLength, characterSet, null)
            .ToString().ShouldBe(expected);
    }

    [Fact]
    public void a_non_default_collation_is_read_back()
    {
        FirebirdColumnType.FromCatalog(37, 0, null, null, 20, "UTF8", "UNICODE_CI")
            .ToString().ShouldBe("VARCHAR(20) CHARACTER SET UTF8 COLLATE UNICODE_CI");
    }

    [Fact]
    public void an_unknown_legacy_type_is_named_by_its_code()
    {
        FirebirdColumnType.FromCatalog(9, null, null, null, null, null, null).ToString().ShouldBe("UNKNOWN(9)");
    }

    [Theory]
    [InlineData("INT", "INTEGER")]
    [InlineData("CHARACTER VARYING(10)", "VARCHAR(10) CHARACTER SET UTF8")]
    [InlineData("BINARY(16)", "CHAR(16) CHARACTER SET OCTETS")]
    [InlineData("NUMERIC", "NUMERIC(9,0)")]
    [InlineData("NUMERIC(18,4)", "NUMERIC(18,4)")]
    [InlineData("VARCHAR(10)", "VARCHAR(10) CHARACTER SET NONE COLLATE UNICODE_CI")]
    [InlineData("VARCHAR(10) COLLATE UTF8", "VARCHAR(10) CHARACTER SET UTF8")]
    [InlineData("BLOB SUB_TYPE TEXT", "BLOB SUB_TYPE TEXT CHARACTER SET UTF8")]
    [InlineData("BLOB", "BLOB SUB_TYPE BINARY")]
    public void a_model_is_satisfied_by_the_column_created_from_it(string model, string catalog)
    {
        FirebirdColumnType.Parse(model).IsSatisfiedBy(FirebirdColumnType.Parse(catalog)).ShouldBeTrue();
    }

    /// <summary>
    ///     weasel#644: what the model states is compared, and only that.
    /// </summary>
    [Theory]
    [InlineData("VARCHAR(10)", "VARCHAR(20) CHARACTER SET UTF8")]
    [InlineData("NUMERIC(18,4)", "NUMERIC(18,2)")]
    [InlineData("NUMERIC(18,4)", "NUMERIC(15,4)")]
    [InlineData("DECIMAL(18,4)", "NUMERIC(18,4)")]
    [InlineData("VARCHAR(10) CHARACTER SET UTF8", "VARCHAR(10) CHARACTER SET NONE")]
    [InlineData("VARCHAR(10) COLLATE UNICODE_CI", "VARCHAR(10) CHARACTER SET UTF8")]
    [InlineData("BLOB SUB_TYPE TEXT", "BLOB SUB_TYPE BINARY")]
    [InlineData("TIMESTAMP", "TIMESTAMP WITH TIME ZONE")]
    [InlineData("DECFLOAT(16)", "DECFLOAT(34)")]
    [InlineData("INTEGER", "BIGINT")]
    [InlineData("CHAR(10)", "VARCHAR(10)")]
    public void a_model_is_not_satisfied_by_a_different_column(string model, string catalog)
    {
        FirebirdColumnType.Parse(model).IsSatisfiedBy(FirebirdColumnType.Parse(catalog)).ShouldBeFalse();
    }

    [Theory]
    [InlineData("VARCHAR(20)", "VARCHAR(10) CHARACTER SET UTF8", true)]
    [InlineData("VARCHAR(10)", "VARCHAR(20) CHARACTER SET UTF8", false)]
    [InlineData("CHAR(20)", "CHAR(10) CHARACTER SET UTF8", true)]
    [InlineData("VARCHAR(20) CHARACTER SET NONE", "VARCHAR(10) CHARACTER SET UTF8", false)]
    [InlineData("VARCHAR(20) COLLATE UNICODE_CI", "VARCHAR(10) CHARACTER SET UTF8", false)]
    [InlineData("VARCHAR(20)", "CHAR(10) CHARACTER SET UTF8", false)]
    [InlineData("INTEGER", "SMALLINT", true)]
    [InlineData("BIGINT", "INTEGER", true)]
    [InlineData("SMALLINT", "INTEGER", false)]
    [InlineData("NUMERIC(18,4)", "NUMERIC(9,4)", true)]
    [InlineData("NUMERIC(18,2)", "NUMERIC(18,4)", false)]
    [InlineData("NUMERIC(9,4)", "NUMERIC(18,4)", false)]
    [InlineData("DOUBLE PRECISION", "FLOAT", true)]
    [InlineData("INTEGER", "VARCHAR(10) CHARACTER SET UTF8", false)]
    [InlineData("VARCHAR(10)", "INTEGER", false)]
    [InlineData("BLOB SUB_TYPE TEXT", "BLOB SUB_TYPE BINARY", false)]
    public void only_a_widening_can_be_altered_in_place(string model, string catalog, bool alterable)
    {
        FirebirdColumnType.Parse(model).CanAlterFrom(FirebirdColumnType.Parse(catalog)).ShouldBe(alterable);
    }

    [Fact]
    public void float_matching_follows_the_server()
    {
        var model = FirebirdColumnType.Parse("FLOAT(10)", Firebird3);
        model.IsSatisfiedBy(FirebirdColumnType.FromCatalog(27, null, null, null, null, null, null)).ShouldBeTrue();

        FirebirdColumnType.Parse("FLOAT(10)", Firebird5)
            .IsSatisfiedBy(FirebirdColumnType.FromCatalog(10, null, null, null, null, null, null)).ShouldBeTrue();
    }
}
