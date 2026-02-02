using MySqlConnector;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.MySql.Tests;

public class MySqlProviderTests
{
    [Fact]
    public void engine_name_is_mysql()
    {
        MySqlProvider.EngineName.ShouldBe("MySql");
    }

    [Fact]
    public void has_singleton_instance()
    {
        MySqlProvider.Instance.ShouldNotBeNull();
    }

    [Theory]
    [InlineData(typeof(string), "VARCHAR(255)")]
    [InlineData(typeof(bool), "TINYINT(1)")]
    [InlineData(typeof(int), "INT")]
    [InlineData(typeof(long), "BIGINT")]
    [InlineData(typeof(short), "SMALLINT")]
    [InlineData(typeof(byte), "TINYINT UNSIGNED")]
    [InlineData(typeof(decimal), "DECIMAL(18,2)")]
    [InlineData(typeof(double), "DOUBLE")]
    [InlineData(typeof(float), "FLOAT")]
    [InlineData(typeof(DateTime), "DATETIME")]
    [InlineData(typeof(DateTimeOffset), "DATETIME")]
    [InlineData(typeof(TimeSpan), "TIME")]
    [InlineData(typeof(Guid), "CHAR(36)")]
    [InlineData(typeof(byte[]), "BLOB")]
    public void maps_clr_type_to_database_type(Type clrType, string expectedDbType)
    {
        var dbType = MySqlProvider.Instance.GetDatabaseType(clrType, EnumStorage.AsInteger);
        dbType.ShouldBe(expectedDbType);
    }

    [Theory]
    [InlineData(typeof(int?), "INT")]
    [InlineData(typeof(bool?), "TINYINT(1)")]
    [InlineData(typeof(long?), "BIGINT")]
    public void maps_nullable_types_to_underlying_type(Type clrType, string expectedDbType)
    {
        var dbType = MySqlProvider.Instance.GetDatabaseType(clrType, EnumStorage.AsInteger);
        dbType.ShouldBe(expectedDbType);
    }

    [Fact]
    public void enum_as_integer_returns_int()
    {
        var dbType = MySqlProvider.Instance.GetDatabaseType(typeof(DayOfWeek), EnumStorage.AsInteger);
        dbType.ShouldBe("INT");
    }

    [Fact]
    public void enum_as_string_returns_varchar()
    {
        var dbType = MySqlProvider.Instance.GetDatabaseType(typeof(DayOfWeek), EnumStorage.AsString);
        dbType.ShouldBe("VARCHAR(100)");
    }

    [Theory]
    [InlineData("INTEGER", "INT")]
    [InlineData("BOOLEAN", "TINYINT(1)")]
    [InlineData("BOOL", "TINYINT(1)")]
    [InlineData("TEXT", "VARCHAR(255)")]
    [InlineData("STRING", "VARCHAR(255)")]
    [InlineData("REAL", "FLOAT")]
    public void convert_synonyms(string input, string expected)
    {
        MySqlProvider.Instance.ConvertSynonyms(input).ShouldBe(expected);
    }

    [Theory]
    [InlineData("CASCADE", CascadeAction.Cascade)]
    [InlineData("NO ACTION", CascadeAction.NoAction)]
    [InlineData("SET NULL", CascadeAction.SetNull)]
    [InlineData("RESTRICT", CascadeAction.Restrict)]
    public void read_cascade_action(string input, CascadeAction expected)
    {
        MySqlProvider.ReadAction(input).ShouldBe(expected);
    }

    [Fact]
    public void parse_creates_mysql_object_name()
    {
        var name = MySqlProvider.Instance.Parse("mydb", "mytable");
        name.ShouldBeOfType<MySqlObjectName>();
        name.Schema.ShouldBe("mydb");
        name.Name.ShouldBe("mytable");
    }

    [Fact]
    public void add_application_name_to_connection_string()
    {
        var connectionString = "Server=localhost;Database=test;";
        var result = MySqlProvider.Instance.AddApplicationNameToConnectionString(connectionString, "MyApp");

        result.ShouldContain("Application Name=MyApp");
    }

    [Fact]
    public void to_parameter_type_for_known_type()
    {
        var dbType = MySqlProvider.Instance.ToParameterType(typeof(int));
        dbType.ShouldBe(MySqlDbType.Int32);
    }

    [Fact]
    public void to_parameter_type_for_string()
    {
        var dbType = MySqlProvider.Instance.ToParameterType(typeof(string));
        dbType.ShouldBe(MySqlDbType.VarChar);
    }

    [Fact]
    public void to_parameter_type_for_guid()
    {
        var dbType = MySqlProvider.Instance.ToParameterType(typeof(Guid));
        dbType.ShouldBe(MySqlDbType.Guid);
    }

    [Fact]
    public void arrays_throw_not_supported()
    {
        Should.Throw<NotSupportedException>(() =>
            MySqlProvider.Instance.GetDatabaseType(typeof(int[]), EnumStorage.AsInteger));
    }
}
