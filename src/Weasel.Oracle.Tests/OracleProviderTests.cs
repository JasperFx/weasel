using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Oracle.Tests;

public class OracleProviderTests
{
    [Fact]
    public void default_schema_is_WEASEL()
    {
        OracleProvider.Instance.DefaultDatabaseSchemaName.ShouldBe("WEASEL");
    }

    [Theory]
    [InlineData(typeof(string), "VARCHAR2(4000)")]
    [InlineData(typeof(bool), "NUMBER(1)")]
    [InlineData(typeof(int), "NUMBER(10)")]
    [InlineData(typeof(long), "NUMBER(19)")]
    [InlineData(typeof(decimal), "NUMBER")]
    [InlineData(typeof(double), "BINARY_DOUBLE")]
    [InlineData(typeof(float), "BINARY_FLOAT")]
    [InlineData(typeof(DateTime), "DATE")]
    [InlineData(typeof(DateTimeOffset), "TIMESTAMP WITH TIME ZONE")]
    [InlineData(typeof(Guid), "RAW(16)")]
    [InlineData(typeof(byte[]), "BLOB")]
    public void get_database_type_for_clr_type(Type clrType, string expectedDatabaseType)
    {
        OracleProvider.Instance.GetDatabaseType(clrType, Core.EnumStorage.AsInteger)
            .ShouldBe(expectedDatabaseType);
    }

    [Theory]
    [InlineData("INT", "NUMBER(10)")]
    [InlineData("INTEGER", "NUMBER(10)")]
    [InlineData("SMALLINT", "NUMBER(5)")]
    [InlineData("BIGINT", "NUMBER(19)")]
    [InlineData("BOOLEAN", "NUMBER(1)")]
    [InlineData("BOOL", "NUMBER(1)")]
    [InlineData("TEXT", "VARCHAR2(4000)")]
    [InlineData("STRING", "VARCHAR2(4000)")]
    [InlineData("FLOAT", "BINARY_FLOAT")]
    [InlineData("DOUBLE", "BINARY_DOUBLE")]
    [InlineData("NUMBER", "NUMBER")]
    public void convert_synonyms(string input, string expected)
    {
        OracleProvider.Instance.ConvertSynonyms(input).ShouldBe(expected);
    }

    [Theory]
    [InlineData("CASCADE", CascadeAction.Cascade)]
    [InlineData("NO ACTION", CascadeAction.NoAction)]
    [InlineData("NO_ACTION", CascadeAction.NoAction)]
    [InlineData("SET NULL", CascadeAction.SetNull)]
    [InlineData("SET_NULL", CascadeAction.SetNull)]
    [InlineData("SET DEFAULT", CascadeAction.SetDefault)]
    [InlineData("SET_DEFAULT", CascadeAction.SetDefault)]
    public void read_cascade_action(string description, CascadeAction expected)
    {
        OracleProvider.ReadAction(description).ShouldBe(expected);
    }
}
