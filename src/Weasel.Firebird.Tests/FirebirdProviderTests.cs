using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Types;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Firebird.Tests;

public class FirebirdProviderTests
{
    [Theory]
    [InlineData(typeof(string), "VARCHAR(255)")]
    [InlineData(typeof(bool), "BOOLEAN")]
    [InlineData(typeof(byte), "SMALLINT")]
    [InlineData(typeof(short), "SMALLINT")]
    [InlineData(typeof(int), "INTEGER")]
    [InlineData(typeof(long), "BIGINT")]
    [InlineData(typeof(decimal), "NUMERIC(18,4)")]
    [InlineData(typeof(double), "DOUBLE PRECISION")]
    [InlineData(typeof(float), "FLOAT")]
    [InlineData(typeof(DateTime), "TIMESTAMP")]
    [InlineData(typeof(DateOnly), "DATE")]
    [InlineData(typeof(TimeOnly), "TIME")]
    [InlineData(typeof(TimeSpan), "TIME")]
    [InlineData(typeof(DateTimeOffset), "TIMESTAMP WITH TIME ZONE")]
    [InlineData(typeof(Guid), "CHAR(16) CHARACTER SET OCTETS")]
    [InlineData(typeof(byte[]), "BLOB SUB_TYPE BINARY")]
    public void maps_clr_types_to_firebird_column_types(Type type, string expected)
    {
        FirebirdProvider.Instance.GetDatabaseType(type, EnumStorage.AsInteger).ShouldBe(expected);
    }

    [Fact]
    public void a_nullable_maps_like_its_underlying_type()
    {
        FirebirdProvider.Instance.GetDatabaseType(typeof(int?), EnumStorage.AsInteger).ShouldBe("INTEGER");
        FirebirdProvider.Instance.GetDatabaseType(typeof(Guid?), EnumStorage.AsInteger)
            .ShouldBe("CHAR(16) CHARACTER SET OCTETS");
    }

    [Fact]
    public void an_enum_is_stored_as_an_integer_or_as_its_name()
    {
        FirebirdProvider.Instance.GetDatabaseType(typeof(DayOfWeek), EnumStorage.AsInteger).ShouldBe("INTEGER");
        FirebirdProvider.Instance.GetDatabaseType(typeof(DayOfWeek), EnumStorage.AsString).ShouldBe("VARCHAR(100)");
    }

    [Fact]
    public void a_type_with_no_column_of_its_own_is_stored_as_json_text()
    {
        FirebirdProvider.Instance.GetDatabaseType(typeof(FirebirdProviderTests), EnumStorage.AsInteger)
            .ShouldBe("BLOB SUB_TYPE TEXT");
        FirebirdProvider.Instance.GetDatabaseType(typeof(List<string>), EnumStorage.AsInteger)
            .ShouldBe("BLOB SUB_TYPE TEXT");
    }

    [Fact]
    public void an_array_other_than_bytes_has_no_column_type()
    {
        Should.Throw<NotSupportedException>(() =>
            FirebirdProvider.Instance.GetDatabaseType(typeof(int[]), EnumStorage.AsInteger));
    }

    [Theory]
    [InlineData(typeof(string), FbDbType.VarChar)]
    [InlineData(typeof(bool), FbDbType.Boolean)]
    [InlineData(typeof(int), FbDbType.Integer)]
    [InlineData(typeof(long), FbDbType.BigInt)]
    [InlineData(typeof(Guid), FbDbType.Guid)]
    [InlineData(typeof(DateTimeOffset), FbDbType.TimeStampTZ)]
    [InlineData(typeof(byte[]), FbDbType.Binary)]
    [InlineData(typeof(int?), FbDbType.Integer)]
    [InlineData(typeof(DayOfWeek), FbDbType.Integer)]
    public void maps_clr_types_to_parameter_types(Type type, FbDbType expected)
    {
        FirebirdProvider.Instance.ToParameterType(type).ShouldBe(expected);
    }

    [Fact]
    public void an_array_is_refused_as_a_parameter()
    {
        Should.Throw<NotSupportedException>(() => FirebirdProvider.Instance.ToParameterType(typeof(int[])));
    }

    [Fact]
    public void the_default_schema_is_the_firebird_6_default()
    {
        FirebirdProvider.Instance.DefaultDatabaseSchemaName.ShouldBe("PUBLIC");
    }

    [Fact]
    public void parses_into_a_firebird_object_name()
    {
        FirebirdProvider.Instance.Parse("orders").ShouldBeOfType<FirebirdObjectName>();
    }

    [Fact]
    public void adds_the_application_name_to_the_connection_string()
    {
        var connectionString = FirebirdProvider.Instance.AddApplicationNameToConnectionString(
            "DataSource=localhost;Database=/data/weasel.fdb;User=SYSDBA;Password=pw", "weasel-app");

        new FbConnectionStringBuilder(connectionString).ApplicationName.ShouldBe("weasel-app");
    }

    [Theory]
    [InlineData("RESTRICT", CascadeAction.Restrict)]
    [InlineData("NO ACTION", CascadeAction.NoAction)]
    [InlineData("CASCADE", CascadeAction.Cascade)]
    [InlineData("SET NULL", CascadeAction.SetNull)]
    [InlineData("SET DEFAULT", CascadeAction.SetDefault)]
    [InlineData("cascade   ", CascadeAction.Cascade)]
    [InlineData(null, CascadeAction.NoAction)]
    public void reads_the_catalog_spelling_of_a_foreign_key_rule(string? rule, CascadeAction expected)
    {
        FirebirdProvider.ReadAction(rule).ShouldBe(expected);
    }

    /// <summary>
    ///     FirebirdClient cannot bind a <see cref="DateTimeOffset" />; it rejects the value outright.
    /// </summary>
    [Fact]
    public void a_date_time_offset_parameter_travels_as_a_zoned_timestamp()
    {
        var command = new FbCommand();
        var instant = new DateTimeOffset(2026, 9, 29, 10, 0, 0, TimeSpan.FromHours(3));

        var parameter = command.AddNamedParameter("at", instant);

        parameter.Value.ShouldBeOfType<FbZonedDateTime>();
        ((FbZonedDateTime)parameter.Value!).DateTime.ShouldBe(instant.UtcDateTime);
        parameter.FbDbType.ShouldBe(FbDbType.TimeStampTZ);
    }
}
