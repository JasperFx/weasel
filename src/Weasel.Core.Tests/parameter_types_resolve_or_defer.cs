using System.Data;
using Microsoft.Data.SqlClient;
using FirebirdSql.Data.FirebirdClient;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using Npgsql;
using NpgsqlTypes;
using Oracle.ManagedDataAccess.Client;
using Shouldly;
using Weasel.Firebird;
using Weasel.MySql;
using Weasel.Oracle;
using Weasel.Postgresql;
using Weasel.Sqlite;
using Weasel.SqlServer;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     Every provider has to resolve a parameter type it knows, and defer the ones it does not
///     rather than answering with a catch-all that the server will have to convert.
/// </summary>
/// <remarks>
///     <para>
///     weasel#758. Five of the seven providers resolved through a private helper that ended in
///     <c>?? &lt;catch-all&gt;</c> — <c>DbType.Object</c> on the generic provider,
///     <c>SqlDbType.Variant</c> on SQL Server, <c>SqliteType.Text</c>, <c>MySqlDbType.VarChar</c>,
///     <c>OracleDbType.Varchar2</c>. Because that could never return null, <c>determineParameterType</c>
///     always reported success and <b>every branch after its first was dead code</b>, which took
///     weasel#404's "null means unmapped, defer to the driver" with it. Only PostgreSQL and Firebird
///     fell through, which is why the divergence read as a PostgreSQL quirk.
///     </para>
///     <para>
///     The cost is not cosmetic. SQL Server sends <c>DbType.Object</c> / <c>SqlDbType.Variant</c> as
///     <c>sql_variant</c>, and sql_variant outranks every ordinary type in data type precedence — so
///     comparing one against a column converts the <b>column</b>, once per row. Measured on SQL
///     Server 2025, <c>where id = @p</c> against a <c>uniqueidentifier</c> primary key went from a
///     Clustered Index Seek to a Clustered Index Scan carrying
///     <c>CONVERT_IMPLICIT(sql_variant,[id],0)</c>. Every type the memo missed paid that, which on
///     SQL Server meant <see cref="Guid" /> and also <see cref="short" />, <see cref="byte" />,
///     <see cref="float" />, <see cref="char" />, <see cref="DateOnly" /> and <see cref="TimeOnly" />.
///     </para>
///     <para>
///     This is a cross-provider suite rather than one test per provider because the defect was a
///     single line copied seven times, and the two providers that had it right are the evidence for
///     what right looks like. A per-provider test would have been written against the provider's own
///     behaviour and would have pinned the bug.
///     </para>
/// </remarks>
public class parameter_types_resolve_or_defer
{
    public class UnmappedTarget
    {
    }

    private enum Flavor
    {
        Sweet,
        Salty,
        Strawberry
    }

    /// <summary>
    ///     Named rather than passed as a delegate so the theory data stays serializable: xUnit
    ///     cannot serialize a <see cref="Func{T, TResult}" />, and a theory it cannot serialize
    ///     collapses to a single test under the supervisor.
    /// </summary>
    public static TheoryData<string> Providers() =>
        new() { "PostgreSQL", "SqlServer", "Sqlite", "MySql", "Oracle", "Firebird" };

    /// <summary>
    ///     The parameter type a provider answers for a CLR type, or null when it has none —
    ///     in which case the caller leaves the parameter untyped and the driver infers it.
    /// </summary>
    private static object? TryGetDbType(string provider, Type type) => provider switch
    {
        "PostgreSQL" => PostgresqlProvider.Instance.TryGetDbType(type),
        "SqlServer" => SqlServerProvider.Instance.TryGetDbType(type),
        "Sqlite" => SqliteProvider.Instance.TryGetDbType(type),
        "MySql" => MySqlProvider.Instance.TryGetDbType(type),
        "Oracle" => OracleProvider.Instance.TryGetDbType(type),
        "Firebird" => FirebirdProvider.Instance.TryGetDbType(type),
        _ => throw new ArgumentOutOfRangeException(nameof(provider), provider)
    };

    [Theory]
    [MemberData(nameof(Providers))]
    public void defers_an_unmapped_type_instead_of_naming_a_catch_all(string provider)
    {
        TryGetDbType(provider, typeof(UnmappedTarget))
            .ShouldBeNull($"{provider} should defer an unmapped type to the driver");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void still_resolves_a_mapped_type(string provider)
    {
        // The guard above is only meaningful if deferral did not become the answer for everything.
        TryGetDbType(provider, typeof(int)).ShouldNotBeNull($"{provider} should still resolve int");
        TryGetDbType(provider, typeof(string)).ShouldNotBeNull($"{provider} should still resolve string");
    }

    /// <summary>
    ///     The generic provider is the one weasel#758 was filed against: it had no
    ///     <see cref="Guid" /> mapping at all, so both the typed <c>AppendParameter(Guid)</c>
    ///     overload and the inferred <c>AddNamedParameter(name, (object)guid)</c> produced
    ///     <c>DbType.Object</c>, which SqlClient sends as sql_variant.
    /// </summary>
    [Fact]
    public void generic_builder_types_a_guid_as_a_guid()
    {
        var command = new SqlCommand();
        var builder = new DbCommandBuilder(command);

        builder.AppendParameter(Guid.NewGuid());
        builder.AddNamedParameter("inferred", (object)Guid.NewGuid());
        builder.AddNamedParameter("typed", Guid.NewGuid());

        foreach (SqlParameter parameter in command.Parameters)
        {
            parameter.DbType.ShouldBe(DbType.Guid);
            parameter.SqlDbType.ShouldBe(SqlDbType.UniqueIdentifier);
        }
    }

    /// <summary>
    ///     weasel#759. Both the generic provider and SQL Server mapped <see cref="DateTime" /> to a
    ///     date-only parameter type, so SqlClient sent it as <c>date</c> and the time of day was
    ///     silently dropped — a bound <c>2026-10-30T13:45:12Z</c> came back
    ///     <c>2026-10-30T00:00:00</c>. Measured against SQL Server 2025, <c>DateTime2</c> is the only
    ///     one of the three candidates that round trips exactly, and it seeks against a
    ///     <c>datetime</c> column as well as a <c>datetime2</c> one.
    /// </summary>
    [Fact]
    public void generic_builder_keeps_the_time_of_day_on_a_datetime()
    {
        var command = new SqlCommand();
        var builder = new DbCommandBuilder(command);

        builder.AddNamedParameter("inferred", (object)DateTime.UtcNow);
        builder.AddParameters(new Dictionary<string, object?> { ["fromDictionary"] = DateTime.UtcNow });
        builder.AddParameters(new { fromProperties = DateTime.UtcNow });

        foreach (SqlParameter parameter in command.Parameters)
        {
            parameter.DbType.ShouldBe(DbType.DateTime2);
            parameter.SqlDbType.ShouldBe(SqlDbType.DateTime2);
        }
    }

    [Fact]
    public void sql_server_keeps_the_time_of_day_on_a_datetime()
    {
        SqlServerProvider.Instance.ToParameterType(typeof(DateTime)).ShouldBe(SqlDbType.DateTime2);
    }

    /// <summary>
    ///     The database type a migration generates is deliberately left alone, so that fixing the
    ///     parameter type does not hand everyone a schema diff on an existing column. The comparison
    ///     stays seek-clean either way: a <c>datetime2</c> parameter against a <c>datetime</c> column
    ///     seeks with no implicit conversion.
    /// </summary>
    [Fact]
    public void the_generated_column_type_for_a_datetime_does_not_change()
    {
        SqlServerProvider.Instance.GetDatabaseType(typeof(DateTime), EnumStorage.AsInteger)
            .ShouldBe("datetime");
    }

    /// <summary>
    ///     The types that fell to SQL Server's <c>Variant</c> catch-all and therefore paid
    ///     weasel#758's conversion on the column. Measured: <c>where v = @p</c> against a
    ///     <c>smallint</c> primary key scanned with <c>CONVERT_IMPLICIT(sql_variant,[v],0)</c> when
    ///     the parameter was bound as <c>Variant</c>, and seeked when bound as <c>SmallInt</c>.
    /// </summary>
    [Theory]
    [InlineData(typeof(short), SqlDbType.SmallInt)]
    [InlineData(typeof(byte), SqlDbType.TinyInt)]
    [InlineData(typeof(float), SqlDbType.Real)]
    [InlineData(typeof(char), SqlDbType.NChar)]
    [InlineData(typeof(DateOnly), SqlDbType.Date)]
    [InlineData(typeof(TimeOnly), SqlDbType.Time)]
    [InlineData(typeof(Guid), SqlDbType.UniqueIdentifier)]
    public void sql_server_does_not_leave_a_primitive_to_the_variant_catch_all(Type clrType, SqlDbType expected)
    {
        SqlServerProvider.Instance.ToParameterType(clrType).ShouldBe(expected);
    }

    /// <summary>
    ///     weasel#763 closed the two providers weasel#760 left alone, and they did not close the same
    ///     way. MySQL takes both types cleanly, measured by round trip against MySQL 8.0. Oracle takes
    ///     neither, against any candidate <c>OracleDbType</c>, so it refuses with an explanation
    ///     instead -- and refuses rather than defers, because an untyped <c>OracleParameter</c> will
    ///     not even hold the value, so there is nothing for ODP.NET to infer from. The per-provider
    ///     measurements live in <c>Weasel.MySql.Tests.dateonly_and_timeonly_round_trip</c> and
    ///     <c>Weasel.Oracle.Tests.dateonly_and_timeonly_are_refused</c>; this is the cross-provider
    ///     picture, so that a later sweep cannot quietly make them agree the wrong way.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void every_provider_has_an_answer_for_date_only_and_time_only(string provider)
    {
        if (provider == "Oracle")
        {
            foreach (var type in new[] { typeof(DateOnly), typeof(TimeOnly) })
            {
                Should.Throw<NotSupportedException>(() => TryGetDbType(provider, type))
                    .Message.ShouldContain("Oracle cannot bind");
            }

            return;
        }

        TryGetDbType(provider, typeof(DateOnly))
            .ShouldNotBeNull($"{provider} should resolve DateOnly");
        TryGetDbType(provider, typeof(TimeOnly))
            .ShouldNotBeNull($"{provider} should resolve TimeOnly");
    }

    /// <summary>
    ///     weasel#403 let a provider choose a parameter type from the <em>value</em> rather than its
    ///     type, which PostgreSQL needs because <c>timestamptz</c> versus <c>timestamp</c> follows
    ///     <see cref="DateTime.Kind" />. Both <c>AddParameters</c> overloads went around that seam by
    ///     calling <c>ToParameterType</c> on the type, so a UTC <see cref="DateTime" /> in a parameter
    ///     bag was bound as <c>timestamp without time zone</c> and Npgsql threw outright — while the
    ///     same value passed one at a time through <c>AddNamedParameter</c> worked.
    /// </summary>
    [Fact]
    public void add_parameters_resolves_from_the_value_not_the_type()
    {
        var utc = new DateTime(2026, 10, 30, 13, 45, 12, DateTimeKind.Utc);
        var unspecified = new DateTime(2026, 10, 30, 13, 45, 12, DateTimeKind.Unspecified);

        TypeOf(b => b.AddParameters(new Dictionary<string, object?> { ["d"] = utc }))
            .ShouldBe(NpgsqlDbType.TimestampTz);
        TypeOf(b => b.AddParameters(new { d = utc }))
            .ShouldBe(NpgsqlDbType.TimestampTz);

        // The Kind really is what decides it, rather than DateTime having been remapped wholesale.
        TypeOf(b => b.AddParameters(new Dictionary<string, object?> { ["d"] = unspecified }))
            .ShouldBe(NpgsqlDbType.Timestamp);

        static NpgsqlDbType TypeOf(Action<Postgresql.CommandBuilder> configure)
        {
            var builder = new Postgresql.CommandBuilder();
            configure(builder);
            return builder.Compile().Parameters[0].NpgsqlDbType;
        }
    }

    /// <summary>
    ///     A null carries no value to key off, so <c>AddParameters</c> still resolves it from the
    ///     declared type. Otherwise a <c>DateTime?</c> property holding null would arrive untyped.
    /// </summary>
    [Fact]
    public void add_parameters_types_a_null_from_the_declared_type()
    {
        var command = new SqlCommand();
        var builder = new DbCommandBuilder(command);

        builder.AddParameters(new { when = (DateTime?)null });

        var parameter = (SqlParameter)command.Parameters[0];
        parameter.SqlDbType.ShouldBe(SqlDbType.DateTime2);
        parameter.Value.ShouldBe(DBNull.Value);
    }

    /// <summary>
    ///     weasel#762. The two builders disagree about what a .NET <see cref="string" /> is, and the
    ///     disagreement is deliberate rather than an oversight -- so it is pinned here, because the
    ///     obvious "tidy up" in either direction is a worse bug than the one it fixes.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     <c>nvarchar</c> outranks <c>varchar</c> in SQL Server's data type precedence, so the
    ///     direction decides which side of a comparison is converted. Measured on SQL Server 2025
    ///     (17.0.4045.5) by reading the cached plan out of <c>sys.dm_exec_query_plan</c>, against an
    ///     indexed column of 5,000 rows:
    ///     </para>
    ///     <list type="table">
    ///         <item><term>VarChar vs varchar(100)</term><description>Index Seek</description></item>
    ///         <item><term>VarChar vs nvarchar(100)</term><description>Index Seek, the convert on the
    ///         parameter</description></item>
    ///         <item><term>NVarChar vs nvarchar(100)</term><description>Index Seek</description></item>
    ///         <item><term>NVarChar vs varchar(100)</term><description>Index <em>Scan</em>, with
    ///         CONVERT_IMPLICIT(nvarchar(100), [probe]) on the <em>column</em></description></item>
    ///     </list>
    ///     <para>
    ///     So <c>Weasel.SqlServer</c>'s non-Unicode default is the safe direction -- it seeks against
    ///     either column shape -- and the generic builder's Unicode default is the dangerous one,
    ///     specifically against the <c>varchar</c> column Weasel's own DDL generates. Both are left
    ///     alone: <c>DbType.AnsiString</c> on the generic provider would truncate non-Latin1 text,
    ///     and an <c>nvarchar</c> column type would be a schema diff for every existing column.
    ///     </para>
    /// </remarks>
    [Fact]
    public void the_two_builders_disagree_about_strings_deliberately()
    {
        SqlServerProvider.Instance.ToParameterType(typeof(string)).ShouldBe(SqlDbType.VarChar);
        SqlServerProvider.Instance.GetDatabaseType(typeof(string), EnumStorage.AsInteger)
            .ShouldBe("varchar(100)");

        // The generic provider's DbType.String is what SqlClient turns into nvarchar.
        var command = new SqlCommand();
        new DbCommandBuilder(command).AddNamedParameter("s", (object)"abc");
        var parameter = (SqlParameter)command.Parameters[0];
        parameter.DbType.ShouldBe(DbType.String);
        parameter.SqlDbType.ShouldBe(SqlDbType.NVarChar);
    }

    /// <summary>
    ///     And the one deliberate exception to it: a <see cref="char" /> is Unicode on SQL Server,
    ///     because it had no mapping at all before weasel#760 and so there was no behaviour to
    ///     preserve -- the parameter and the column it generates agree with each other, and an
    ///     <c>nchar</c> cannot lose a character. weasel#762 settled this alongside the string.
    /// </summary>
    [Fact]
    public void a_char_is_unicode_and_agrees_with_its_own_column_type()
    {
        SqlServerProvider.Instance.ToParameterType(typeof(char)).ShouldBe(SqlDbType.NChar);
        SqlServerProvider.Instance.GetDatabaseType(typeof(char), EnumStorage.AsInteger)
            .ShouldBe("nchar(1)");
    }

    /// <summary>
    ///     weasel#761. This test used to be <c>enums_and_arrays_keep_the_behaviour_they_had</c>, and
    ///     it is rewritten rather than deleted because the behaviour it pinned has deliberately
    ///     changed — which is what it existed to force. It pinned the two explicit catch-alls
    ///     weasel#760 left behind when it made the fall-through honest, so that whoever changed one
    ///     did it on purpose. This is that change.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     What each provider sent for an enum before, measured against the column its own
    ///     <c>GetDatabaseType</c> generates rather than reasoned about. Three of the six did not
    ///     merely convert the column — they threw outright, which is why there was no behaviour here
    ///     worth preserving:
    ///     </para>
    ///     <list type="table">
    ///         <item><term>SQL Server, Variant</term><description>threw against both columns:
    ///         "Implicit conversion from data type sql_variant to int is not allowed"</description></item>
    ///         <item><term>MySQL, VarChar</term><description>sent the name; threw against the
    ///         AsInteger column: "Incorrect integer value: 'Strawberry'"</description></item>
    ///         <item><term>Oracle, Varchar2</term><description>sent the name; threw ORA-01722 against
    ///         the AsInteger column</description></item>
    ///         <item><term>SQLite, Text</term><description>stored the integer either way; type
    ///         affinity makes it moot</description></item>
    ///         <item><term>PostgreSQL / Firebird, Integer</term><description>correct for AsInteger,
    ///         wrong for AsString</description></item>
    ///     </list>
    /// </remarks>
    [Theory]
    [MemberData(nameof(Providers))]
    public void an_enum_resolves_as_an_integer_unless_told_otherwise(string provider)
    {
        // The parameterless overload has no EnumStorage in scope, so it assumes AsInteger -- the
        // JasperFx default -- and must not diverge from the explicit call.
        ToDbType(provider, typeof(Flavor))
            .ShouldBe(ToDbType(provider, typeof(int)));

        ToDbType(provider, typeof(Flavor), EnumStorage.AsInteger)
            .ShouldBe(ToDbType(provider, typeof(int)));

        ToDbType(provider, typeof(Flavor), EnumStorage.AsString)
            .ShouldBe(ToDbType(provider, typeof(string)));
    }

    /// <summary>
    ///     The invariant that keeps the two overloads from drifting: every provider routes its
    ///     parameterless enum branch through the same rule, rather than naming a constant of its own.
    ///     That is how the catch-alls came to disagree in the first place.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void the_parameterless_overload_agrees_with_as_integer(string provider)
    {
        foreach (var type in new[] { typeof(Flavor), typeof(Flavor?), typeof(LongFlavor), typeof(ByteFlavor) })
        {
            ToDbType(provider, type)
                .ShouldBe(ToDbType(provider, type, EnumStorage.AsInteger), $"{provider}, {type.Name}");
        }
    }

    /// <summary>
    ///     An enum is resolved through its <em>underlying</em> type, not through <see cref="int" />
    ///     flatly, so a <c>long</c>-backed enum carrying a value past <see cref="int.MaxValue" />
    ///     cannot be narrowed on the way out. Asserted where the provider has a distinct mapping for
    ///     the underlying type; SQLite maps every integer width to <c>INTEGER</c>, so there is
    ///     nothing to tell apart there.
    /// </summary>
    [Fact]
    public void a_long_backed_enum_resolves_through_its_underlying_type()
    {
        SqlServerProvider.Instance.ToParameterType(typeof(LongFlavor)).ShouldBe(SqlDbType.BigInt);
        MySqlProvider.Instance.ToParameterType(typeof(LongFlavor)).ShouldBe(MySqlDbType.Int64);
        OracleProvider.Instance.ToParameterType(typeof(LongFlavor)).ShouldBe(OracleDbType.Int64);
        FirebirdProvider.Instance.ToParameterType(typeof(LongFlavor)).ShouldBe(FbDbType.BigInt);

        // And the value a narrowing would have lost really is out of int's range.
        ((long)LongFlavor.Huge).ShouldBeGreaterThan(int.MaxValue);
    }

    /// <summary>
    ///     A nullable enum resolves the same way as its enum, rather than falling through the
    ///     nullable branch to something else.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void a_nullable_enum_resolves_like_its_enum(string provider)
    {
        ToDbType(provider, typeof(Flavor?)).ShouldBe(ToDbType(provider, typeof(Flavor)));
        ToDbType(provider, typeof(Flavor?), EnumStorage.AsString)
            .ShouldBe(ToDbType(provider, typeof(string)));
    }

    /// <summary>
    ///     An enum resolved from a <em>value</em> rather than a type goes through the same rule, so
    ///     the value-aware path cannot be the one that keeps the old answer. weasel#403 added that
    ///     seam, and weasel#758 found that <c>AddParameters</c> had gone around it.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void an_enum_value_resolves_the_same_way_as_its_type(string provider)
    {
        ForValue(provider, Flavor.Strawberry).ShouldBe(ToDbType(provider, typeof(int)));
        ForValue(provider, Flavor.Strawberry, EnumStorage.AsString)
            .ShouldBe(ToDbType(provider, typeof(string)));
    }

    /// <summary>
    ///     Arrays now defer instead of resolving to a character catch-all. The refusal the dead
    ///     branch carried ("Sql Server does not support arrays") is deliberately <em>not</em> turned
    ///     on: deferring leaves the parameter untyped for the driver, which is both the established
    ///     meaning of "unmapped" here (weasel#404) and the answer that does not override a driver
    ///     able to handle an array on its own.
    /// </summary>
    /// <remarks>
    ///     PostgreSQL still maps an array properly, as an <c>Array | element</c> composite, and
    ///     Firebird still refuses one outright. Neither is changed — the four providers that silently
    ///     bound an <c>int[]</c> as text are.
    /// </remarks>
    [Theory]
    [InlineData("SqlServer")]
    [InlineData("Sqlite")]
    [InlineData("MySql")]
    [InlineData("Oracle")]
    public void an_array_defers_to_the_driver(string provider)
    {
        TryGetDbType(provider, typeof(int[]))
            .ShouldBeNull($"{provider} should leave an int[] to the driver");

        // ToParameterType still throws for an unmapped type, which is what it has meant since #760.
        Should.Throw<NotSupportedException>(() => ToDbType(provider, typeof(int[])));
    }

    [Fact]
    public void postgresql_still_maps_an_array_and_firebird_still_refuses_one()
    {
        PostgresqlProvider.Instance.ToParameterType(typeof(int[]))
            .ShouldBe(NpgsqlDbType.Array | NpgsqlDbType.Integer);

        Should.Throw<NotSupportedException>(() => FirebirdProvider.Instance.ToParameterType(typeof(int[])));
    }

    /// <summary>
    ///     SQLite's <c>byte[]</c> is in the memo as a blob and must not be caught by the change
    ///     above — the memo is consulted first, so an array branch could never have reached it, but
    ///     that ordering is exactly the kind of thing a later edit breaks.
    /// </summary>
    [Fact]
    public void a_byte_array_is_still_a_blob_rather_than_a_deferred_array()
    {
        SqliteProvider.Instance.ToParameterType(typeof(byte[])).ShouldBe(SqliteType.Blob);
        SqlServerProvider.Instance.ToParameterType(typeof(byte[])).ShouldBe(SqlDbType.Binary);
        MySqlProvider.Instance.ToParameterType(typeof(byte[])).ShouldBe(MySqlDbType.Blob);
        OracleProvider.Instance.ToParameterType(typeof(byte[])).ShouldBe(OracleDbType.Blob);
        PostgresqlProvider.Instance.ToParameterType(typeof(byte[])).ShouldBe(NpgsqlDbType.Bytea);
    }

    private static object ToDbType(string provider, Type type) => provider switch
    {
        "PostgreSQL" => PostgresqlProvider.Instance.ToParameterType(type),
        "SqlServer" => SqlServerProvider.Instance.ToParameterType(type),
        "Sqlite" => SqliteProvider.Instance.ToParameterType(type),
        "MySql" => MySqlProvider.Instance.ToParameterType(type),
        "Oracle" => OracleProvider.Instance.ToParameterType(type),
        "Firebird" => FirebirdProvider.Instance.ToParameterType(type),
        _ => throw new ArgumentOutOfRangeException(nameof(provider), provider)
    };

    private static object ToDbType(string provider, Type type, EnumStorage enumStyle) => provider switch
    {
        "PostgreSQL" => PostgresqlProvider.Instance.ToParameterType(type, enumStyle),
        "SqlServer" => SqlServerProvider.Instance.ToParameterType(type, enumStyle),
        "Sqlite" => SqliteProvider.Instance.ToParameterType(type, enumStyle),
        "MySql" => MySqlProvider.Instance.ToParameterType(type, enumStyle),
        "Oracle" => OracleProvider.Instance.ToParameterType(type, enumStyle),
        "Firebird" => FirebirdProvider.Instance.ToParameterType(type, enumStyle),
        _ => throw new ArgumentOutOfRangeException(nameof(provider), provider)
    };

    private static object? ForValue(string provider, object value) => provider switch
    {
        "PostgreSQL" => PostgresqlProvider.Instance.TryGetDbTypeForValue(value),
        "SqlServer" => SqlServerProvider.Instance.TryGetDbTypeForValue(value),
        "Sqlite" => SqliteProvider.Instance.TryGetDbTypeForValue(value),
        "MySql" => MySqlProvider.Instance.TryGetDbTypeForValue(value),
        "Oracle" => OracleProvider.Instance.TryGetDbTypeForValue(value),
        "Firebird" => FirebirdProvider.Instance.TryGetDbTypeForValue(value),
        _ => throw new ArgumentOutOfRangeException(nameof(provider), provider)
    };

    private static object? ForValue(string provider, object value, EnumStorage enumStyle) => provider switch
    {
        "PostgreSQL" => PostgresqlProvider.Instance.TryGetDbTypeForValue(value, enumStyle),
        "SqlServer" => SqlServerProvider.Instance.TryGetDbTypeForValue(value, enumStyle),
        "Sqlite" => SqliteProvider.Instance.TryGetDbTypeForValue(value, enumStyle),
        "MySql" => MySqlProvider.Instance.TryGetDbTypeForValue(value, enumStyle),
        "Oracle" => OracleProvider.Instance.TryGetDbTypeForValue(value, enumStyle),
        "Firebird" => FirebirdProvider.Instance.TryGetDbTypeForValue(value, enumStyle),
        _ => throw new ArgumentOutOfRangeException(nameof(provider), provider)
    };

    public enum LongFlavor : long
    {
        Huge = 5_000_000_000L
    }

    public enum ByteFlavor : byte
    {
        Tiny = 7
    }

    /// <summary>
    ///     A <see cref="DBNull" /> still resolves, rather than deferring. Leaving a null untyped is
    ///     not the same thing as leaving an unmapped value untyped — several drivers cannot infer
    ///     anything from <see cref="DBNull.Value" />.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void dbnull_still_resolves(string provider)
    {
        TryGetDbType(provider, typeof(DBNull)).ShouldNotBeNull($"{provider} should resolve DBNull");
    }

    /// <summary>
    ///     Every driver the generic <see cref="DbCommandBuilder" /> wraps carries the two parameter
    ///     types this change introduces. Measured rather than assumed, because weasel#758 flagged the
    ///     shared-driver risk explicitly: <c>DbType.Guid</c> had to be checked against Npgsql,
    ///     MySqlConnector and Microsoft.Data.Sqlite, not just SqlClient.
    /// </summary>
    [Fact]
    public void every_wrapped_driver_carries_the_new_db_types()
    {
        Assert(new SqlCommand(), DbType.Guid, p => ((SqlParameter)p).SqlDbType.ToString(), "UniqueIdentifier");
        Assert(new NpgsqlCommand(), DbType.Guid, p => ((NpgsqlParameter)p).NpgsqlDbType.ToString(), "Uuid");
        Assert(new MySqlCommand(), DbType.Guid, p => ((MySqlParameter)p).MySqlDbType.ToString(), "Guid");
        // SQLite has no uuid type; TEXT is how Weasel.Sqlite stores one, so it still matches a row.
        Assert(new SqliteCommand(), DbType.Guid, p => ((SqliteParameter)p).SqliteType.ToString(), "Text");

        Assert(new SqlCommand(), DbType.DateTime2, p => ((SqlParameter)p).SqlDbType.ToString(), "DateTime2");
        Assert(new NpgsqlCommand(), DbType.DateTime2, p => ((NpgsqlParameter)p).NpgsqlDbType.ToString(), "Timestamp");
        Assert(new MySqlCommand(), DbType.DateTime2, p => ((MySqlParameter)p).MySqlDbType.ToString(), "DateTime");
        Assert(new SqliteCommand(), DbType.DateTime2, p => ((SqliteParameter)p).SqliteType.ToString(), "Text");

        static void Assert(System.Data.Common.DbCommand command, DbType dbType,
            Func<System.Data.Common.DbParameter, string> nativeType, string expected)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = "p";
            parameter.DbType = dbType;
            nativeType(parameter).ShouldBe(expected);
        }
    }

    /// <summary>
    ///     Firebird never had the catch-all, so it is the control: its behaviour must not move.
    /// </summary>
    [Fact]
    public void firebird_still_refuses_an_array()
    {
        Should.Throw<NotSupportedException>(() => FirebirdProvider.Instance.ToParameterType(typeof(int[])));
    }
}
