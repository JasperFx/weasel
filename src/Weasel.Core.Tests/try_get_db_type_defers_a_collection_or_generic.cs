using System.Data;
using FirebirdSql.Data.FirebirdClient;
using NpgsqlTypes;
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
///     A <c>Try*</c> parameter-type lookup answers "I don't know" for a type it has no mapping for;
///     it does not throw.
/// </summary>
/// <remarks>
///     <para>
///     weasel#826. <c>SqlServerProvider.TryGetDbType(typeof(List&lt;int&gt;))</c> threw
///     <see cref="NotSupportedException" /> — <c>Can't infer SqlDbType for type List`1[T]</c>. The
///     open-generic step of <c>determineParameterType</c> recursed into the generic definition through
///     <c>ToParameterType</c>, which throws for an unmapped type, so the exception escaped the
///     <c>Try</c> wrapper. The nullable step recursed the same way, and PostgreSQL's array, sequence and
///     range steps did too. Every provider shared the shape; PostgreSQL only looked fine for
///     <c>List&lt;int&gt;</c> because its sequence step maps that as an <c>integer[]</c> first.
///     </para>
///     <para>
///     Two refusals are deliberate and stay: Firebird refuses an array outright, and Oracle refuses
///     <see cref="DateOnly" /> / <see cref="TimeOnly" /> (pinned in
///     <see cref="parameter_types_resolve_or_defer" />). Those are a provider saying "no", not "I don't
///     know". <c>ToParameterType</c> still throws for an unmapped type — that is what it means.
///     </para>
/// </remarks>
public class try_get_db_type_defers_a_collection_or_generic
{
    public class Box<T>
    {
    }

    public class Registered<T>
    {
    }

    public class Plain
    {
    }

    public struct PlainStruct
    {
    }

    // Enumerates itself, as Newtonsoft's JToken does: a sequence step that recursed into the element
    // without a self check would never stop.
    public class SelfEnumerating: IEnumerable<SelfEnumerating>
    {
        public IEnumerator<SelfEnumerating> GetEnumerator() => Enumerable.Empty<SelfEnumerating>().GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private static readonly Dictionary<string, (Type Type, Func<object>? Value)> Cases = new()
    {
        ["List<int>"] = (typeof(List<int>), () => new List<int> { 1 }),
        ["List<string>"] = (typeof(List<string>), () => new List<string> { "a" }),
        ["IList<int>"] = (typeof(IList<int>), null),
        ["IEnumerable<int>"] = (typeof(IEnumerable<int>), null),
        ["HashSet<Guid>"] = (typeof(HashSet<Guid>), () => new HashSet<Guid> { Guid.NewGuid() }),
        ["Dictionary<string,int>"] = (typeof(Dictionary<string, int>), () => new Dictionary<string, int> { ["a"] = 1 }),
        ["Box<int>"] = (typeof(Box<int>), () => new Box<int>()),
        ["int[]"] = (typeof(int[]), () => new[] { 1 }),
        ["List<Plain>"] = (typeof(List<Plain>), () => new List<Plain>()),
        ["Plain[]"] = (typeof(Plain[]), () => new Plain[0]),
        ["PlainStruct?"] = (typeof(PlainStruct?), null),
        ["NpgsqlRange<Plain>"] = (typeof(NpgsqlRange<Plain>), null),
        ["SelfEnumerating"] = (typeof(SelfEnumerating), () => new SelfEnumerating()),
        ["List<>"] = (typeof(List<>), null)
    };

    private static readonly string[] AllProviders =
        ["Generic", "PostgreSQL", "SqlServer", "Sqlite", "MySql", "Oracle", "Firebird"];

    public static TheoryData<string, string> ProvidersAndTypes()
    {
        var data = new TheoryData<string, string>();
        foreach (var provider in AllProviders)
        {
            foreach (var name in Cases.Keys)
            {
                // Firebird's array refusal is deliberate; firebird_still_refuses_an_array_through_try pins it.
                if (provider == "Firebird" && Cases[name].Type.IsArray)
                {
                    continue;
                }

                data.Add(provider, name);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(ProvidersAndTypes))]
    public void try_paths_do_not_throw(string provider, string typeName)
    {
        var (type, value) = Cases[typeName];

        Should.NotThrow(() => TryGetDbType(provider, type), $"{provider} TryGetDbType({typeName})");
        Should.NotThrow(() => TryGetDbType(provider, type, EnumStorage.AsInteger),
            $"{provider} TryGetDbType({typeName}, AsInteger)");
        Should.NotThrow(() => TryGetDbType(provider, type, EnumStorage.AsString),
            $"{provider} TryGetDbType({typeName}, AsString)");

        if (value != null)
        {
            var instance = value();
            Should.NotThrow(() => TryGetDbTypeForValue(provider, instance),
                $"{provider} TryGetDbTypeForValue({typeName})");
        }
    }

    [Fact]
    public void sql_server_defers_a_list_of_int()
    {
        SqlServerProvider.Instance.TryGetDbType(typeof(List<int>)).ShouldBeNull();
        SqlServerProvider.Instance.TryGetDbTypeForValue(new List<int> { 1, 2 }).ShouldBeNull();

        // ToParameterType still refuses an unmapped type, and now names the type it was asked about
        // rather than the open generic definition.
        Should.Throw<NotSupportedException>(() => SqlServerProvider.Instance.ToParameterType(typeof(List<int>)))
            .Message.ShouldContain("System.Int32");
    }

    [Fact]
    public void a_mapping_registered_for_an_open_generic_still_covers_its_closings()
    {
        SqlServerProvider.Instance.RegisterMapping(typeof(Registered<>), "varchar(10)", SqlDbType.VarChar);

        SqlServerProvider.Instance.TryGetDbType(typeof(Registered<int>)).ShouldBe(SqlDbType.VarChar);
        SqlServerProvider.Instance.TryGetDbType(typeof(Registered<Guid>)).ShouldBe(SqlDbType.VarChar);
    }

    [Fact]
    public void postgresql_still_maps_a_sequence_of_a_mapped_element()
    {
        PostgresqlProvider.Instance.TryGetDbType(typeof(List<int>))
            .ShouldBe(NpgsqlDbType.Array | NpgsqlDbType.Integer);
        PostgresqlProvider.Instance.TryGetDbType(typeof(IList<int>))
            .ShouldBe(NpgsqlDbType.Array | NpgsqlDbType.Integer);
        PostgresqlProvider.Instance.TryGetDbType(typeof(HashSet<Guid>))
            .ShouldBe(NpgsqlDbType.Array | NpgsqlDbType.Uuid);
        PostgresqlProvider.Instance.TryGetDbType(typeof(NpgsqlRange<int>))
            .ShouldBe(NpgsqlDbType.Range | NpgsqlDbType.Integer);
        PostgresqlProvider.Instance.TryGetDbType(typeof(int?))
            .ShouldBe(NpgsqlDbType.Integer);
    }

    [Fact]
    public void postgresql_defers_a_sequence_of_an_unmapped_element()
    {
        PostgresqlProvider.Instance.TryGetDbType(typeof(List<Plain>)).ShouldBeNull();
        PostgresqlProvider.Instance.TryGetDbType(typeof(Plain[])).ShouldBeNull();
        PostgresqlProvider.Instance.TryGetDbType(typeof(Dictionary<string, int>)).ShouldBeNull();
    }

    [Fact]
    public void firebird_still_refuses_an_array_through_try()
    {
        // An active refusal rather than an unknown: Firebird has no array parameters to defer to.
        Should.Throw<NotSupportedException>(() => FirebirdProvider.Instance.TryGetDbType(typeof(int[])));
        FirebirdProvider.Instance.TryGetDbType(typeof(List<int>)).ShouldBeNull();
    }

    private static object? TryGetDbType(string provider, Type type) => provider switch
    {
        "Generic" => DbDatabaseProvider.Instance.TryGetDbType(type),
        "PostgreSQL" => PostgresqlProvider.Instance.TryGetDbType(type),
        "SqlServer" => SqlServerProvider.Instance.TryGetDbType(type),
        "Sqlite" => SqliteProvider.Instance.TryGetDbType(type),
        "MySql" => MySqlProvider.Instance.TryGetDbType(type),
        "Oracle" => OracleProvider.Instance.TryGetDbType(type),
        "Firebird" => FirebirdProvider.Instance.TryGetDbType(type),
        _ => throw new ArgumentOutOfRangeException(nameof(provider), provider)
    };

    private static object? TryGetDbType(string provider, Type type, EnumStorage enumStyle) => provider switch
    {
        "Generic" => DbDatabaseProvider.Instance.TryGetDbType(type, enumStyle),
        "PostgreSQL" => PostgresqlProvider.Instance.TryGetDbType(type, enumStyle),
        "SqlServer" => SqlServerProvider.Instance.TryGetDbType(type, enumStyle),
        "Sqlite" => SqliteProvider.Instance.TryGetDbType(type, enumStyle),
        "MySql" => MySqlProvider.Instance.TryGetDbType(type, enumStyle),
        "Oracle" => OracleProvider.Instance.TryGetDbType(type, enumStyle),
        "Firebird" => FirebirdProvider.Instance.TryGetDbType(type, enumStyle),
        _ => throw new ArgumentOutOfRangeException(nameof(provider), provider)
    };

    private static object? TryGetDbTypeForValue(string provider, object value) => provider switch
    {
        "Generic" => DbDatabaseProvider.Instance.TryGetDbTypeForValue(value),
        "PostgreSQL" => PostgresqlProvider.Instance.TryGetDbTypeForValue(value),
        "SqlServer" => SqlServerProvider.Instance.TryGetDbTypeForValue(value),
        "Sqlite" => SqliteProvider.Instance.TryGetDbTypeForValue(value),
        "MySql" => MySqlProvider.Instance.TryGetDbTypeForValue(value),
        "Oracle" => OracleProvider.Instance.TryGetDbTypeForValue(value),
        "Firebird" => FirebirdProvider.Instance.TryGetDbTypeForValue(value),
        _ => throw new ArgumentOutOfRangeException(nameof(provider), provider)
    };
}
