using System.Data;
using Npgsql;
using NpgsqlTypes;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests;

[Collection("PostgresqlProviderTests")]
public class PostgresqlProviderTests
{
    [Fact]
    public void execute_to_db_type_as_int()
    {
        PostgresqlProvider.Instance.ToParameterType(typeof(int)).ShouldBe(NpgsqlDbType.Integer);
        PostgresqlProvider.Instance.ToParameterType(typeof(int?)).ShouldBe(NpgsqlDbType.Integer);
    }

    [Fact]
    public void execute_to_db_custom_mappings_resolve()
    {
        NpgsqlTypeMapper.Mappings[NpgsqlDbType.Varchar] =
            new NpgsqlTypeMapping(
                NpgsqlDbType.Varchar,
                DbType.String,
                "varchar",
                typeof(MappedTarget)
            );

        PostgresqlProvider.Instance.ToParameterType(typeof(MappedTarget)).ShouldBe(NpgsqlDbType.Varchar);
        ShouldThrowExtensions.ShouldThrow<Exception>(() =>
            PostgresqlProvider.Instance.ToParameterType(typeof(UnmappedTarget)));
    }


    [Fact]
    public void execute_get_pg_type_default_mappings_resolve()
    {
        PostgresqlProvider.Instance.GetDatabaseType(typeof(long), EnumStorage.AsString).ShouldBe("bigint");
        PostgresqlProvider.Instance.GetDatabaseType(typeof(DateTime), EnumStorage.AsString)
            .ShouldBe("timestamp without time zone");
    }

    [Fact]
    public void execute_get_pg_type_custom_mappings_resolve_or_default_to_jsonb()
    {
        NpgsqlConnection.GlobalTypeMapper.MapComposite<MappedTarget>("varchar");

        PostgresqlProvider.Instance.GetDatabaseType(typeof(MappedTarget), EnumStorage.AsString).ShouldBe("varchar");
        PostgresqlProvider.Instance.GetDatabaseType(typeof(UnmappedTarget), EnumStorage.AsString).ShouldBe("jsonb");
    }

    [Fact]
    public void execute_has_type_mapping_resolves_custom_types()
    {
        NpgsqlConnection.GlobalTypeMapper.MapComposite<MappedTarget>("varchar");

        PostgresqlProvider.Instance.HasTypeMapping(typeof(MappedTarget)).ShouldBeTrue();
        PostgresqlProvider.Instance.HasTypeMapping(typeof(UnmappedTarget)).ShouldBeFalse();
    }

    public class MappedTarget
    {
    }

    public class UnmappedTarget
    {
    }

    [Fact]
    public void canonicizesql_supports_tabs_as_whitespace()
    {
        var noTabsCanonized =
            "\r\nDECLARE\r\n  final_version uuid;\r\nBEGIN\r\nINSERT INTO table(\"data\", \"mt_dotnet_type\", \"id\", \"mt_version\", mt_last_modified) VALUES (doc, docDotNetType, docId, docVersion, transaction_timestamp())\r\n  ON CONFLICT ON CONSTRAINT pk_table\r\n  DO UPDATE SET \"data\" = doc, \"mt_dotnet_type\" = docDotNetType, \"mt_version\" = docVersion, mt_last_modified = transaction_timestamp();\r\n\r\n  SELECT mt_version FROM table into final_version WHERE id = docId;\r\n  RETURN final_version;\r\nEND;\r\n"
                .CanonicizeSql();
        var tabsCanonized =
            "\r\nDECLARE\r\n\tfinal_version uuid;\r\nBEGIN\r\n\tINSERT INTO table(\"data\", \"mt_dotnet_type\", \"id\", \"mt_version\", mt_last_modified)\r\n\tVALUES (doc, docDotNetType, docId, docVersion, transaction_timestamp())\r\n\t\tON CONFLICT ON CONSTRAINT pk_table\r\n\t\t\tDO UPDATE SET \"data\" = doc, \"mt_dotnet_type\" = docDotNetType, \"mt_version\" = docVersion, mt_last_modified = transaction_timestamp();\r\n\r\n\tSELECT mt_version FROM table into final_version WHERE id = docId;\r\n\r\n\tRETURN final_version;\r\nEND;\r\n"
                .CanonicizeSql();
        noTabsCanonized.ShouldBe(tabsCanonized);
    }

    [Fact]
    public void replaces_multiple_spaces_with_new_string()
    {
        var inputString = "Darth        Maroon the   First";
        var expectedString = "Darth Maroon the First";
        inputString.ReplaceMultiSpace(" ").ShouldBe(expectedString);
    }

    [Fact]
    public void table_columns_should_match_raw_types()
    {
        var serialAsInt = new TableColumn("id", "serial");
        serialAsInt.ShouldBe(new TableColumn("id", "int"));

        var bigserialAsBigint = new TableColumn("id", "bigserial");
        bigserialAsBigint.ShouldBe(new TableColumn("id", "bigint"));

        var smallserialAsSmallint = new TableColumn("id", "smallserial");
        smallserialAsSmallint.ShouldBe(new TableColumn("id", "smallint"));

        var varchararrAsArray = new TableColumn("comments", "varchar[]");
        varchararrAsArray.ShouldBe(new TableColumn("comments", "array"));

        var charactervaryingAsArray = new TableColumn("comments", "character varying[]");
        charactervaryingAsArray.ShouldBe(new TableColumn("comments", "array"));

        var textarrayAsArray = new TableColumn("comments", "text[]");
        textarrayAsArray.ShouldBe(new TableColumn("comments", "array"));
    }

    [Theory]
    [InlineData("character varying", "varchar")]
    [InlineData("varchar", "varchar")]
    [InlineData("boolean", "boolean")]
    [InlineData("bool", "boolean")]
    [InlineData("integer", "int")]
    [InlineData("serial", "int")]
    [InlineData("bigserial", "bigint")]
    [InlineData("smallserial", "smallint")]
    [InlineData("integer[]", "int[]")]
    [InlineData("decimal", "decimal")]
    [InlineData("numeric", "decimal")]
    [InlineData("timestamp without time zone", "timestamp")]
    [InlineData("timestamp with time zone", "timestamptz")]
    [InlineData("array", "array")]
    [InlineData("character varying[]", "array")]
    [InlineData("varchar[]", "array")]
    [InlineData("text[]", "array")]
    public void convert_synonyms(string type, string synonym)
    {
        PostgresqlProvider.Instance.ConvertSynonyms(type).ShouldBe(synonym);
    }

    [Fact]
    public void execute_to_parameter_type_with_open_generics()
    {
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithOpenGeneric<>), "varchar", NpgsqlDbType.Varchar);
        PostgresqlProvider.Instance.ToParameterType(typeof(WithOpenGeneric<>)).ShouldBe(NpgsqlDbType.Varchar);
    }

    [Fact]
    public void execute_to_parameter_type_with_closed_generics_falls_back_to_open()
    {
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithOpenGeneric<>), "varchar", NpgsqlDbType.Varchar);
        PostgresqlProvider.Instance.ToParameterType(typeof(WithOpenGeneric<int>)).ShouldBe(NpgsqlDbType.Varchar);
    }

    [Fact]
    public void execute_to_parameter_type_with_closed_generics()
    {
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithClosedGeneric<int>), "varchar", NpgsqlDbType.Varchar);
        PostgresqlProvider.Instance.ToParameterType(typeof(WithClosedGeneric<int>)).ShouldBe(NpgsqlDbType.Varchar);
    }

    [Fact]
    public void execute_to_parameter_type_with_closed_generics_open_generic()
    {
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithClosedGeneric<int>), "varchar", NpgsqlDbType.Varchar);
        Action act = () => PostgresqlProvider.Instance.ToParameterType(typeof(WithClosedGeneric<>));
        act.ShouldThrow<NotSupportedException>();
    }

    [Fact]
    public void execute_to_parameter_type_with_closed_generics_generic_override()
    {
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithClosedGeneric<int>), "varchar", NpgsqlDbType.Varchar);
        Action act = () => PostgresqlProvider.Instance.ToParameterType(typeof(WithClosedGeneric<string>));
        act.ShouldThrow<NotSupportedException>();
    }

    [Fact]
    public void execute_to_parameter_type_with_closed_generics_overrides_definitions_with_open_generic()
    {
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithMixedGeneric<>), "varchar", NpgsqlDbType.Varchar);
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithMixedGeneric<int>), "integer", NpgsqlDbType.Integer);
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithMixedGeneric<bool>), "boolean", NpgsqlDbType.Boolean);
        PostgresqlProvider.Instance.ToParameterType(typeof(WithMixedGeneric<int>)).ShouldBe(NpgsqlDbType.Integer);
        PostgresqlProvider.Instance.ToParameterType(typeof(WithMixedGeneric<bool>)).ShouldBe(NpgsqlDbType.Boolean);
        PostgresqlProvider.Instance.ToParameterType(typeof(WithMixedGeneric<string>)).ShouldBe(NpgsqlDbType.Varchar);
    }

    [Fact]
    public void execute_to_db_type_with_open_generics()
    {
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithOpenGeneric<>), "varchar", NpgsqlDbType.Varchar);
        PostgresqlProvider.Instance.GetDatabaseType(typeof(WithOpenGeneric<>), EnumStorage.AsInteger).ShouldBe("varchar");
    }

    [Fact]
    public void execute_to_db_type_with_closed_generics_falls_back_to_open()
    {
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithOpenGeneric<>), "varchar", NpgsqlDbType.Varchar);
        PostgresqlProvider.Instance.GetDatabaseType(typeof(WithOpenGeneric<int>), EnumStorage.AsInteger).ShouldBe("varchar");
    }

    [Fact]
    public void execute_to_db_type_with_closed_generics()
    {
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithClosedGeneric<int>), "varchar", NpgsqlDbType.Varchar);
        PostgresqlProvider.Instance.GetDatabaseType(typeof(WithClosedGeneric<int>), EnumStorage.AsInteger).ShouldBe("varchar");
    }

    [Fact]
    public void execute_to_db_type_with_closed_generics_open_generic()
    {
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithClosedGeneric<int>), "varchar", NpgsqlDbType.Varchar);
        PostgresqlProvider.Instance.GetDatabaseType(typeof(WithClosedGeneric<>), EnumStorage.AsInteger).ShouldBe("jsonb");
    }

    [Fact]
    public void execute_to_db_type_with_closed_generics_generic_override()
    {
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithClosedGeneric<int>), "varchar", NpgsqlDbType.Varchar);
        PostgresqlProvider.Instance.GetDatabaseType(typeof(WithClosedGeneric<string>), EnumStorage.AsInteger).ShouldBe("jsonb");
    }

    [Fact]
    public void execute_to_db_type_with_closed_generics_overrides_definitions_with_open_generic()
    {
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithMixedGeneric<>), "varchar", NpgsqlDbType.Varchar);
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithMixedGeneric<int>), "integer", NpgsqlDbType.Integer);
        PostgresqlProvider.Instance.RegisterMapping(typeof(WithMixedGeneric<bool>), "boolean", NpgsqlDbType.Boolean);
        PostgresqlProvider.Instance.GetDatabaseType(typeof(WithMixedGeneric<int>), EnumStorage.AsInteger).ShouldBe("integer");
        PostgresqlProvider.Instance.GetDatabaseType(typeof(WithMixedGeneric<bool>), EnumStorage.AsInteger).ShouldBe("boolean");
        PostgresqlProvider.Instance.GetDatabaseType(typeof(WithMixedGeneric<string>), EnumStorage.AsInteger).ShouldBe("varchar");
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

[CollectionDefinition("PostgresqlProviderTests", DisableParallelization = true)]
public class PostgresqlProviderTestsCollectionDefinition;
