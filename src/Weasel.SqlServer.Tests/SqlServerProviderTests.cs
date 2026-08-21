using System.Data;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.SqlServer.Tests;

public class SqlServerProviderTests
{
    [Fact]
    public void execute_to_db_type_as_int()
    {
        SqlServerProvider.Instance.ToParameterType(typeof(int)).ShouldBe(SqlDbType.Int);
        SqlServerProvider.Instance.ToParameterType(typeof(int?)).ShouldBe(SqlDbType.Int);
    }

    [Fact]
    public void add_application_name_to_connection_string()
    {
        SqlServerProvider.Instance.AddApplicationNameToConnectionString(ConnectionSource.ConnectionString, "ThisApp")
            .ShouldContain("Application Name=ThisApp");
    }


    public class MappedTarget
    {
    }

    public class UnmappedTarget
    {
    }

    [Fact]
    public void execute_to_parameter_type_with_open_generics()
    {
        SqlServerProvider.Instance.RegisterMapping(typeof(WithOpenGeneric<>), "VarChar", SqlDbType.VarChar);
        SqlServerProvider.Instance.ToParameterType(typeof(WithOpenGeneric<>)).ShouldBe(SqlDbType.VarChar);
    }

    [Fact(Skip = "Different backing behaviour than for Postgres")]
    public void execute_to_parameter_type_with_closed_generics_falls_back_to_open()
    {
        SqlServerProvider.Instance.RegisterMapping(typeof(WithOpenGeneric<>), "VarChar", SqlDbType.VarChar);
        SqlServerProvider.Instance.ToParameterType(typeof(WithOpenGeneric<int>)).ShouldBe(SqlDbType.VarChar);
    }

    [Fact]
    public void execute_to_parameter_type_with_closed_generics()
    {
        SqlServerProvider.Instance.RegisterMapping(typeof(WithClosedGeneric<int>), "VarChar", SqlDbType.VarChar);
        SqlServerProvider.Instance.ToParameterType(typeof(WithClosedGeneric<int>)).ShouldBe(SqlDbType.VarChar);
    }

    [Fact(Skip = "Different backing behaviour than for Postgres")]
    public void execute_to_parameter_type_with_closed_generics_open_generic()
    {
        SqlServerProvider.Instance.RegisterMapping(typeof(WithClosedGeneric<int>), "VarChar", SqlDbType.VarChar);
        Action act = () => SqlServerProvider.Instance.ToParameterType(typeof(WithClosedGeneric<>));
        act.ShouldThrow<NotSupportedException>();
    }

    [Fact(Skip = "Different backing behaviour than for Postgres")]
    public void execute_to_parameter_type_with_closed_generics_generic_override()
    {
        SqlServerProvider.Instance.RegisterMapping(typeof(WithClosedGeneric<int>), "VarChar", SqlDbType.VarChar);
        Action act = () => SqlServerProvider.Instance.ToParameterType(typeof(WithClosedGeneric<string>));
        act.ShouldThrow<NotSupportedException>();
    }

    [Fact(Skip = "Different backing behaviour than for Postgres")]
    public void execute_to_parameter_type_with_closed_generics_overrides_definitions_with_open_generic()
    {
        SqlServerProvider.Instance.RegisterMapping(typeof(WithMixedGeneric<>), "VarChar", SqlDbType.VarChar);
        SqlServerProvider.Instance.RegisterMapping(typeof(WithMixedGeneric<int>), "integer", SqlDbType.Int);
        SqlServerProvider.Instance.RegisterMapping(typeof(WithMixedGeneric<bool>), "boolean", SqlDbType.Bit);
        SqlServerProvider.Instance.ToParameterType(typeof(WithMixedGeneric<int>)).ShouldBe(SqlDbType.Int);
        SqlServerProvider.Instance.ToParameterType(typeof(WithMixedGeneric<bool>)).ShouldBe(SqlDbType.Bit);
        SqlServerProvider.Instance.ToParameterType(typeof(WithMixedGeneric<string>)).ShouldBe(SqlDbType.VarChar);
    }

    [Fact]
    public void execute_to_db_type_with_open_generics()
    {
        SqlServerProvider.Instance.RegisterMapping(typeof(WithOpenGeneric<>), "VarChar", SqlDbType.VarChar);
        SqlServerProvider.Instance.GetDatabaseType(typeof(WithOpenGeneric<>), EnumStorage.AsInteger).ShouldBe("VarChar");
    }

    [Fact(Skip = "Different backing behaviour than for Postgres")]
    public void execute_to_db_type_with_closed_generics_falls_back_to_open()
    {
        SqlServerProvider.Instance.RegisterMapping(typeof(WithOpenGeneric<>), "VarChar", SqlDbType.VarChar);
        SqlServerProvider.Instance.GetDatabaseType(typeof(WithOpenGeneric<int>), EnumStorage.AsInteger).ShouldBe("VarChar");
    }

    [Fact]
    public void execute_to_db_type_with_closed_generics()
    {
        SqlServerProvider.Instance.RegisterMapping(typeof(WithClosedGeneric<int>), "VarChar", SqlDbType.VarChar);
        SqlServerProvider.Instance.GetDatabaseType(typeof(WithClosedGeneric<int>), EnumStorage.AsInteger).ShouldBe("VarChar");
    }

    [Fact(Skip = "Different backing behaviour than for Postgres")]
    public void execute_to_db_type_with_closed_generics_open_generic()
    {
        SqlServerProvider.Instance.RegisterMapping(typeof(WithClosedGeneric<int>), "VarChar", SqlDbType.VarChar);
        SqlServerProvider.Instance.GetDatabaseType(typeof(WithClosedGeneric<>), EnumStorage.AsInteger).ShouldBe("nvarchar(max)");
    }

    [Fact(Skip = "Different backing behaviour than for Postgres")]
    public void execute_to_db_type_with_closed_generics_generic_override()
    {
        SqlServerProvider.Instance.RegisterMapping(typeof(WithClosedGeneric<int>), "VarChar", SqlDbType.VarChar);
        SqlServerProvider.Instance.GetDatabaseType(typeof(WithClosedGeneric<string>), EnumStorage.AsInteger).ShouldBe("nvarchar(max)");
    }

    [Fact(Skip = "Different backing behaviour than for Postgres")]
    public void execute_to_db_type_with_closed_generics_overrides_definitions_with_open_generic()
    {
        SqlServerProvider.Instance.RegisterMapping(typeof(WithMixedGeneric<>), "VarChar", SqlDbType.VarChar);
        SqlServerProvider.Instance.RegisterMapping(typeof(WithMixedGeneric<int>), "integer", SqlDbType.Int);
        SqlServerProvider.Instance.RegisterMapping(typeof(WithMixedGeneric<bool>), "boolean", SqlDbType.Bit);
        SqlServerProvider.Instance.GetDatabaseType(typeof(WithMixedGeneric<int>), EnumStorage.AsInteger).ShouldBe("integer");
        SqlServerProvider.Instance.GetDatabaseType(typeof(WithMixedGeneric<bool>), EnumStorage.AsInteger).ShouldBe("boolean");
        SqlServerProvider.Instance.GetDatabaseType(typeof(WithMixedGeneric<string>), EnumStorage.AsInteger).ShouldBe("VarChar");
    }

    public class WithOpenGeneric<T>
    {
    }

    public class WithClosedGeneric<T>
    {
    }

    public class WithMixedGeneric<T>
    {
    }
}
