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
///     A parameter-type lookup runs once per bound parameter of every command, so a lookup the memo
///     can answer has to answer it without allocating. weasel#804.
/// </summary>
/// <remarks>
///     <para>
///     It did not. The memo helpers in <c>DatabaseProvider</c> wrote back through
///     <c>Swap(d =&gt; d.AddOrUpdate(type, ...))</c>, and a lambda capturing a method's parameter makes
///     the compiler allocate its closure at the top of that method -- so a memo <em>hit</em>, which
///     never reached the Swap, still paid 32 bytes on every call, and PostgreSQL paid it twice.
///     The fix is only to keep the writes out of line; a capturing lambda put back into one of those
///     methods fails this.
///     </para>
///     <para>
///     PostgreSQL's enum and array rows are the larger half of the same issue: the memo stored "no
///     Npgsql mapping" as null and could not tell it from "never asked", so those types rescanned the
///     whole mapping registry -- ~1 KB and ~450 ns -- on every lookup.
///     </para>
///     <para>
///     Judged as an average under eight bytes per call rather than exactly zero, so that a one-off
///     allocation by the runtime during the measured loop cannot fail it; anything per-call is at
///     least 24.
///     </para>
/// </remarks>
public class parameter_type_lookups_do_not_allocate
{
    public enum Status
    {
        Open,
        Closed
    }

    private static readonly Dictionary<string, Type> Types = new()
    {
        ["int"] = typeof(int),
        ["string"] = typeof(string),
        ["Guid"] = typeof(Guid),
        ["DateTimeOffset"] = typeof(DateTimeOffset),
        ["int?"] = typeof(int?),
        ["enum"] = typeof(Status),
        ["int[]"] = typeof(int[])
    };

    private static readonly Dictionary<string, Func<Type, bool>> Providers = new()
    {
        ["PostgreSQL"] = t => PostgresqlProvider.Instance.TryGetDbType(t).HasValue,
        ["SqlServer"] = t => SqlServerProvider.Instance.TryGetDbType(t).HasValue,
        ["MySql"] = t => MySqlProvider.Instance.TryGetDbType(t).HasValue,
        ["Oracle"] = t => OracleProvider.Instance.TryGetDbType(t).HasValue,
        ["Sqlite"] = t => SqliteProvider.Instance.TryGetDbType(t).HasValue,
        ["Firebird"] = t => FirebirdProvider.Instance.TryGetDbType(t).HasValue
    };

    public static TheoryData<string, string> Lookups()
    {
        var data = new TheoryData<string, string>();
        foreach (var provider in Providers.Keys)
        {
            foreach (var type in Types.Keys)
            {
                // Firebird refuses an array outright -- it throws rather than resolving or
                // deferring -- so there is no lookup there to measure.
                if (provider == "Firebird" && type == "int[]")
                {
                    continue;
                }

                data.Add(provider, type);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Lookups))]
    public void a_repeated_lookup_does_not_allocate(string provider, string type)
    {
        const int iterations = 1_000;

        var lookup = Providers[provider];
        var clrType = Types[type];

        // Warm the memo, and the JIT, before measuring.
        for (var i = 0; i < 100; i++)
        {
            lookup(clrType);
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < iterations; i++)
        {
            lookup(clrType);
        }

        var perCall = (GC.GetAllocatedBytesForCurrentThread() - before) / (double)iterations;

        perCall.ShouldBeLessThan(8, $"{provider} TryGetDbType({type}) allocated {perCall} bytes per call");
    }
}
