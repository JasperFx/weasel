using System.Data;
using System.Net;
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
    public void add_application_name_to_connection_string()
    {
        PostgresqlProvider.Instance.AddApplicationNameToConnectionString(ConnectionSource.ConnectionString, "ThisApp")
            .ShouldContain("Application Name=ThisApp");
    }

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
        PostgresqlProvider.Instance.RegisterMapping(typeof(MappedTarget), "varchar", NpgsqlDbType.Varchar);

        PostgresqlProvider.Instance.GetDatabaseType(typeof(MappedTarget), EnumStorage.AsString).ShouldBe("varchar");
        PostgresqlProvider.Instance.GetDatabaseType(typeof(UnmappedTarget), EnumStorage.AsString).ShouldBe("jsonb");
    }

    [Fact]
    public void execute_has_type_mapping_resolves_custom_types()
    {
        PostgresqlProvider.Instance.RegisterMapping(typeof(MappedTarget), "varchar", NpgsqlDbType.Varchar);

        PostgresqlProvider.Instance.HasTypeMapping(typeof(MappedTarget)).ShouldBeTrue();
        PostgresqlProvider.Instance.HasTypeMapping(typeof(UnmappedTarget)).ShouldBeFalse();
    }

    public class MappedTarget
    {
    }

    public class UnmappedTarget
    {
    }

    // PostgreSQL picks timestamptz vs timestamp from the *value's* Kind, so a per-type mapping
    // cannot express it. GetDatabaseType stays per-type -- a column has one type -- while a
    // parameter's type has to follow its value. weasel#403.

    [Fact]
    public void to_parameter_type_for_value_uses_timestamptz_for_a_utc_datetime()
    {
        PostgresqlProvider.Instance
            .ToParameterTypeForValue(new DateTime(2026, 7, 30, 12, 0, 0, DateTimeKind.Utc))
            .ShouldBe(NpgsqlDbType.TimestampTz);
    }

    [Theory]
    [InlineData(DateTimeKind.Local)]
    [InlineData(DateTimeKind.Unspecified)]
    public void to_parameter_type_for_value_uses_timestamp_for_a_non_utc_datetime(DateTimeKind kind)
    {
        PostgresqlProvider.Instance
            .ToParameterTypeForValue(new DateTime(2026, 7, 30, 12, 0, 0, kind))
            .ShouldBe(NpgsqlDbType.Timestamp);
    }

    [Fact]
    public void to_parameter_type_for_value_handles_datetime_collections()
    {
        var utc = new DateTime(2026, 7, 30, 12, 0, 0, DateTimeKind.Utc);
        var local = new DateTime(2026, 7, 30, 12, 0, 0, DateTimeKind.Local);

        // arrays and List<T> both arrive as IReadOnlyList<DateTime>
        PostgresqlProvider.Instance.ToParameterTypeForValue(new[] { utc })
            .ShouldBe(NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
        PostgresqlProvider.Instance.ToParameterTypeForValue(new List<DateTime> { utc })
            .ShouldBe(NpgsqlDbType.Array | NpgsqlDbType.TimestampTz);
        PostgresqlProvider.Instance.ToParameterTypeForValue(new[] { local })
            .ShouldBe(NpgsqlDbType.Array | NpgsqlDbType.Timestamp);
        PostgresqlProvider.Instance.ToParameterTypeForValue(Array.Empty<DateTime>())
            .ShouldBe(NpgsqlDbType.Array | NpgsqlDbType.Timestamp);
    }

    [Theory]
    [InlineData("a", NpgsqlDbType.Text)]
    [InlineData(42, NpgsqlDbType.Integer)]
    [InlineData(true, NpgsqlDbType.Boolean)]
    public void to_parameter_type_for_value_still_keys_off_the_clr_type_otherwise(object value,
        NpgsqlDbType expected)
    {
        PostgresqlProvider.Instance.ToParameterTypeForValue(value).ShouldBe(expected);
    }

    [Fact]
    public void column_types_stay_per_type_even_though_parameter_types_are_per_value()
    {
        PostgresqlProvider.Instance.GetDatabaseType(typeof(DateTime), EnumStorage.AsString)
            .ShouldBe("timestamp without time zone");
    }

    [Fact]
    public void add_named_parameter_types_a_utc_datetime_as_timestamptz()
    {
        // The regression: AddNamedParameter used to stamp Timestamp here, and Npgsql then threw
        // "Cannot write DateTime with Kind=UTC to PostgreSQL type 'timestamp without time zone'"
        // at execution time. weasel#403.
        new NpgsqlCommand()
            .AddNamedParameter("n", new DateTime(2026, 7, 30, 12, 0, 0, DateTimeKind.Utc))
            .NpgsqlDbType.ShouldBe(NpgsqlDbType.TimestampTz);
    }

    [Fact]
    public void add_named_parameter_leaves_an_unmapped_type_for_the_driver_to_infer()
    {
        // AddNamedParameter used to call ToParameterType and throw "Can't infer NpgsqlDbType
        // for type ..." here, while the sibling AddParameter accepted the same value happily.
        // Both now defer to Npgsql for anything Weasel has no mapping for. weasel#404.
        var command = new NpgsqlCommand();

        Should.NotThrow(() => command.AddNamedParameter("n", new UnmappedTarget()));
        Should.NotThrow(() => command.AddParameter(new UnmappedTarget()));
    }

    [Fact]
    public void try_get_db_type_for_value_returns_null_for_an_unmapped_type()
    {
        PostgresqlProvider.Instance.TryGetDbTypeForValue(new UnmappedTarget()).ShouldBeNull();
    }

    [Fact]
    public void to_parameter_type_for_value_still_throws_for_an_unmapped_type()
    {
        // The throwing overload is kept for callers that want the diagnostic.
        Should.Throw<NotSupportedException>(() =>
            PostgresqlProvider.Instance.ToParameterTypeForValue(new UnmappedTarget()));
    }

    [Fact]
    public void ipnetwork_resolves_to_cidr()
    {
        PostgresqlProvider.Instance
            .GetDatabaseType(typeof(IPNetwork), EnumStorage.AsString)
            .ShouldBe("cidr");
    }

    [Fact]
    public void ipnetwork_is_claimed_by_exactly_one_mapping()
    {
        // Guards the test above. IPNetwork used to be declared on both the cidr and the inet
        // mapping, and GetTypeMapping breaks a tie with LastOrDefault over a Cache backed by
        // an ImHashMap -- so "cidr" was winning on hash layout, not on anything declared.
        // weasel#405.
        NpgsqlTypeMapper.Mappings
            .Count(mapping => mapping.ClrTypes.Contains(typeof(IPNetwork)))
            .ShouldBe(1);
    }

    [Fact]
    public void no_clr_type_is_claimed_by_more_than_one_mapping()
    {
        // Any CLR type reachable from two mappings has an order-dependent, effectively
        // arbitrary resolution. Keep that structurally impossible rather than relying on
        // enumeration order. weasel#405.
        var doubleClaimed = NpgsqlTypeMapper.Mappings
            .SelectMany(mapping => mapping.ClrTypes.Select(clrType => (clrType, mapping)))
            .GroupBy(x => x.clrType)
            .Where(g => g.Count() > 1)
            .Select(g => $"{g.Key.Name} <- {string.Join(", ", g.Select(x => x.mapping.NpgsqlDbType))}")
            .ToArray();

        doubleClaimed.ShouldBeEmpty();
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

    [Theory]
    [InlineData(typeof(Guid[]), "uuid[]")]
    [InlineData(typeof(int[]), "integer[]")]
    [InlineData(typeof(long[]), "bigint[]")]
    [InlineData(typeof(short[]), "smallint[]")]
    [InlineData(typeof(float[]), "real[]")]
    [InlineData(typeof(double[]), "double precision[]")]
    [InlineData(typeof(string[]), "varchar[]")]
    [InlineData(typeof(bool[]), "boolean[]")]
    [InlineData(typeof(decimal[]), "decimal[]")]
    public void get_database_type_for_array_types(Type type, string expected)
    {
        PostgresqlProvider.Instance.GetDatabaseType(type, EnumStorage.AsInteger).ShouldBe(expected);
    }

    [Theory]
    [InlineData(typeof(Guid[]), NpgsqlDbType.Array | NpgsqlDbType.Uuid)]
    [InlineData(typeof(int[]), NpgsqlDbType.Array | NpgsqlDbType.Integer)]
    [InlineData(typeof(long[]), NpgsqlDbType.Array | NpgsqlDbType.Bigint)]
    [InlineData(typeof(short[]), NpgsqlDbType.Array | NpgsqlDbType.Smallint)]
    [InlineData(typeof(float[]), NpgsqlDbType.Array | NpgsqlDbType.Real)]
    [InlineData(typeof(double[]), NpgsqlDbType.Array | NpgsqlDbType.Double)]
    [InlineData(typeof(string[]), NpgsqlDbType.Array | NpgsqlDbType.Text)]
    [InlineData(typeof(bool[]), NpgsqlDbType.Array | NpgsqlDbType.Boolean)]
    public void to_parameter_type_for_array_types(Type type, NpgsqlDbType expected)
    {
        PostgresqlProvider.Instance.ToParameterType(type).ShouldBe(expected);
    }

    [Fact]
    public void table_columns_should_match_for_uuid_array()
    {
        var uuidArray = new TableColumn("ids", "uuid[]");
        uuidArray.ShouldBe(new TableColumn("ids", "uuid[]"));
    }

    [Fact]
    public void table_columns_should_match_for_integer_array()
    {
        var intArray = new TableColumn("ids", "integer[]");
        intArray.ShouldBe(new TableColumn("ids", "int[]"));
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
    [InlineData("uuid[]", "uuid[]")]
    [InlineData("boolean[]", "boolean[]")]
    [InlineData("bool[]", "boolean[]")]
    [InlineData("decimal[]", "decimal[]")]
    [InlineData("numeric[]", "decimal[]")]
    [InlineData("smallint[]", "smallint[]")]
    [InlineData("bigint[]", "bigint[]")]
    [InlineData("real[]", "real[]")]
    [InlineData("double precision[]", "double precision[]")]
    [InlineData("timestamp without time zone[]", "timestamp[]")]
    [InlineData("timestamp with time zone[]", "timestamptz[]")]
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
