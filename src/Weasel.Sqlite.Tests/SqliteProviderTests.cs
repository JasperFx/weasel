using Shouldly;
using Weasel.Core;
using Weasel.Sqlite;
using Xunit;

namespace Weasel.Sqlite.Tests;

public class SqliteProviderTests
{
    private readonly SqliteProvider provider = SqliteProvider.Instance;

    [Fact]
    public void default_schema_is_main()
    {
        provider.DefaultDatabaseSchemaName.ShouldBe("main");
    }

    [Theory]
    [InlineData(typeof(int), "INTEGER")]
    [InlineData(typeof(long), "INTEGER")]
    [InlineData(typeof(short), "INTEGER")]
    [InlineData(typeof(byte), "INTEGER")]
    [InlineData(typeof(bool), "INTEGER")]
    [InlineData(typeof(string), "TEXT")]
    [InlineData(typeof(char), "TEXT")]
    [InlineData(typeof(float), "REAL")]
    [InlineData(typeof(double), "REAL")]
    [InlineData(typeof(decimal), "REAL")]
    [InlineData(typeof(DateTime), "TEXT")]
    [InlineData(typeof(DateTimeOffset), "TEXT")]
    [InlineData(typeof(Guid), "TEXT")]
    [InlineData(typeof(TimeSpan), "TEXT")]
    [InlineData(typeof(byte[]), "BLOB")]
    public void should_map_dotnet_types_to_sqlite_types(Type dotnetType, string expectedSqliteType)
    {
        var sqliteType = provider.GetDatabaseType(dotnetType, EnumStorage.AsInteger);
        sqliteType.ShouldBe(expectedSqliteType);
    }

    [Fact]
    public void should_map_nullable_types()
    {
        provider.GetDatabaseType(typeof(int?), EnumStorage.AsInteger).ShouldBe("INTEGER");
        provider.GetDatabaseType(typeof(DateTime?), EnumStorage.AsInteger).ShouldBe("TEXT");
        provider.GetDatabaseType(typeof(bool?), EnumStorage.AsInteger).ShouldBe("INTEGER");
    }

    [Fact]
    public void should_map_enums_to_integer_or_text()
    {
        provider.GetDatabaseType(typeof(DayOfWeek), EnumStorage.AsInteger).ShouldBe("INTEGER");
        provider.GetDatabaseType(typeof(DayOfWeek), EnumStorage.AsString).ShouldBe("TEXT");
    }

    [Fact]
    public void should_default_unmapped_types_to_text_for_json_storage()
    {
        // Complex types should map to TEXT for JSON storage
        var type = provider.GetDatabaseType(typeof(CustomType), EnumStorage.AsInteger);
        type.ShouldBe("TEXT");
    }

    [Fact]
    public void should_not_support_array_types_except_byte_array()
    {
        // byte[] is supported as BLOB
        provider.GetDatabaseType(typeof(byte[]), EnumStorage.AsInteger).ShouldBe("BLOB");

        // Other arrays should throw
        Should.Throw<NotSupportedException>(() =>
            provider.GetDatabaseType(typeof(int[]), EnumStorage.AsInteger));

        Should.Throw<NotSupportedException>(() =>
            provider.GetDatabaseType(typeof(string[]), EnumStorage.AsInteger));
    }

    [Theory]
    [InlineData("int", "INTEGER")]
    [InlineData("INTEGER", "INTEGER")]
    [InlineData("varchar", "TEXT")]
    [InlineData("text", "TEXT")]
    [InlineData("double", "REAL")]
    [InlineData("float", "REAL")]
    [InlineData("blob", "BLOB")]
    [InlineData("boolean", "INTEGER")]
    public void convert_synonyms(string synonym, string expected)
    {
        provider.ConvertSynonyms(synonym).ShouldBe(expected);
    }

    [Fact]
    public void read_cascade_actions()
    {
        SqliteProvider.ReadAction("CASCADE").ShouldBe(CascadeAction.Cascade);
        SqliteProvider.ReadAction("NO ACTION").ShouldBe(CascadeAction.NoAction);
        SqliteProvider.ReadAction("SET NULL").ShouldBe(CascadeAction.SetNull);
        SqliteProvider.ReadAction("SET DEFAULT").ShouldBe(CascadeAction.SetDefault);
        SqliteProvider.ReadAction("RESTRICT").ShouldBe(CascadeAction.Restrict);
    }

    private class CustomType
    {
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }
}