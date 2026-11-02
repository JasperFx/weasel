using System.Data;
using Microsoft.Data.SqlClient;
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
        Salty
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
    ///     Enums and arrays keep the catch-all they have always resolved to. Making the fall-through
    ///     honest revived two branches that had been unreachable — an enum would have started binding
    ///     as an integer, and an array would have started throwing the refusal written below it — and
    ///     both are behaviour changes of their own rather than part of weasel#758. This pins them so
    ///     that whoever changes one does it on purpose.
    /// </summary>
    [Fact]
    public void enums_and_arrays_keep_the_behaviour_they_had()
    {
        SqlServerProvider.Instance.ToParameterType(typeof(Flavor)).ShouldBe(SqlDbType.Variant);
        SqlServerProvider.Instance.ToParameterType(typeof(int[])).ShouldBe(SqlDbType.Variant);

        SqliteProvider.Instance.ToParameterType(typeof(Flavor)).ShouldBe(SqliteType.Text);
        SqliteProvider.Instance.ToParameterType(typeof(int[])).ShouldBe(SqliteType.Text);

        MySqlProvider.Instance.ToParameterType(typeof(Flavor)).ShouldBe(MySqlDbType.VarChar);
        MySqlProvider.Instance.ToParameterType(typeof(int[])).ShouldBe(MySqlDbType.VarChar);

        OracleProvider.Instance.ToParameterType(typeof(Flavor)).ShouldBe(OracleDbType.Varchar2);
        OracleProvider.Instance.ToParameterType(typeof(int[])).ShouldBe(OracleDbType.Varchar2);

        // byte[] is in SQLite's memo as a blob and must not be caught by the array branch.
        SqliteProvider.Instance.ToParameterType(typeof(byte[])).ShouldBe(SqliteType.Blob);
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
