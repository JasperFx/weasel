using System.Collections.Frozen;
using System.Data;
using BenchmarkDotNet.Attributes;
using NpgsqlTypes;
using Weasel.Postgresql;
using Weasel.SqlServer;

namespace Weasel.Benchmarks;

public enum BenchmarkStatus
{
    Open,
    Closed
}

/// <summary>
///     The provider parameter-type lookup — <c>TryGetDbType(Type)</c> — for the types a command
///     actually binds. weasel#804.
/// </summary>
/// <remarks>
///     <para>
///         This is the per-parameter cost of the LINQ <c>CommandParameter</c> path on both providers
///         and of <c>AddNamedParameter</c> / <c>AddParameters</c> on every command builder, so it runs
///         once per bound parameter of every command.
///     </para>
///     <para>
///         The <c>FrozenDictionary</c> row is not a Weasel path. It is the ceiling for weasel#804's
///         option (a), "pre-populate and freeze": a <see cref="FrozenDictionary{TKey,TValue}" /> holding
///         SQL Server's answer for the same type, probed directly, with no virtual call, no
///         fall-through chain and no nullable promotion. Whatever (a) could win is the gap between that
///         row and the SQL Server row.
///     </para>
/// </remarks>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class ParameterTypeBenchmarks
{
    private static readonly Dictionary<string, Type> Types = new()
    {
        ["int"] = typeof(int),
        ["long"] = typeof(long),
        ["string"] = typeof(string),
        ["Guid"] = typeof(Guid),
        ["DateTime"] = typeof(DateTime),
        ["DateTimeOffset"] = typeof(DateTimeOffset),
        ["bool"] = typeof(bool),
        ["decimal"] = typeof(decimal),
        ["enum"] = typeof(BenchmarkStatus),
        ["int?"] = typeof(int?),
        ["enum?"] = typeof(BenchmarkStatus?),
        ["int[]"] = typeof(int[]),
        ["string[]"] = typeof(string[])
    };

    private Type _type = null!;
    private FrozenDictionary<Type, SqlDbType?> _frozen = null!;

    [ParamsSource(nameof(TypeNames))]
    public string Type { get; set; } = null!;

    public static IEnumerable<string> TypeNames => Types.Keys;

    [GlobalSetup]
    public void Setup()
    {
        _type = Types[Type];

        // Warm both memos so the first-call miss is not what gets measured.
        foreach (var type in Types.Values)
        {
            PostgresqlProvider.Instance.TryGetDbType(type);
            SqlServerProvider.Instance.TryGetDbType(type);
        }

        _frozen = Types.Values.ToFrozenDictionary(t => t, t => SqlServerProvider.Instance.TryGetDbType(t));
    }

    [Benchmark(Description = "Postgresql TryGetDbType")]
    public NpgsqlDbType? Postgresql()
    {
        return PostgresqlProvider.Instance.TryGetDbType(_type);
    }

    [Benchmark(Baseline = true, Description = "SqlServer TryGetDbType")]
    public SqlDbType? SqlServer()
    {
        return SqlServerProvider.Instance.TryGetDbType(_type);
    }

    [Benchmark(Description = "FrozenDictionary probe (ceiling for option a)")]
    public SqlDbType? Frozen()
    {
        return _frozen.TryGetValue(_type, out var value) ? value : null;
    }
}

/// <summary>
///     The one common parameter type the two providers cannot share a row for: a
///     <see cref="List{T}" />. PostgreSQL binds it as an array through the
///     <c>ImplementedInterfaces</c> scan in <c>determineParameterType</c>; SQL Server's
///     <c>TryGetDbType</c> <em>throws</em> for it (the constructed-generic branch calls
///     <c>ToParameterType</c> on the open generic), so there is nothing on that side to time.
/// </summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class PostgresqlListParameterTypeBenchmarks
{
    [GlobalSetup]
    public void Setup()
    {
        PostgresqlProvider.Instance.TryGetDbType(typeof(List<int>));
        PostgresqlProvider.Instance.TryGetDbType(typeof(List<string>));
    }

    [Benchmark(Description = "Postgresql TryGetDbType(List<int>)")]
    public NpgsqlDbType? ListOfInt()
    {
        return PostgresqlProvider.Instance.TryGetDbType(typeof(List<int>));
    }

    [Benchmark(Description = "Postgresql TryGetDbType(List<string>)")]
    public NpgsqlDbType? ListOfString()
    {
        return PostgresqlProvider.Instance.TryGetDbType(typeof(List<string>));
    }
}
