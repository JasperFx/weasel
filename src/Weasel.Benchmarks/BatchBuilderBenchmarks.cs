using BenchmarkDotNet.Attributes;

namespace Weasel.Benchmarks;

/// <summary>
///     Appending 500 parameters to a batch command.
/// </summary>
/// <remarks>
///     <para>
///         SQL Server's <see cref="Weasel.SqlServer.BatchBuilder" /> names every parameter by its
///         position, which used to be a <c>"p" + count</c> string allocation per parameter.
///         weasel#558 replaced that with the precomputed <see cref="Weasel.Core.ParameterNames" />
///         table (512 entries), so 500 parameters now stay inside the table and the naming itself
///         allocates nothing. This measures where that left the append path — what remains is the
///         driver's own <c>SqlParameter</c> plus the SQL text.
///     </para>
///     <para>
///         PostgreSQL's builder is included as the contrast: it writes <c>$N</c> positionally and
///         never named its parameters, so it never paid the allocation and is unaffected by #558.
///         The gap between the two rows is the cost of naming, not of appending.
///     </para>
///     <para>
///         500 is a deliberate choice: it is inside the 512-entry table. See
///         <see cref="ParameterNamesBenchmarks" /> for what happens past the end of it.
///     </para>
/// </remarks>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class BatchBuilderBenchmarks
{
    private const int ParameterCount = 500;

    private object[] _values = null!;

    [GlobalSetup]
    public void Setup()
    {
        _values = Enumerable.Range(0, ParameterCount).Select(i => (object)("value-" + i)).ToArray();
    }

    [Benchmark(Baseline = true, Description = "SqlServer BatchBuilder.AppendParameter x500")]
    public int SqlServer()
    {
        var builder = new SqlServer.BatchBuilder();
        for (var i = 0; i < _values.Length; i++)
        {
            builder.Append(", ");
            builder.AppendParameter(_values[i]);
        }

        return _values.Length;
    }

    [Benchmark(Description = "Postgresql BatchBuilder.AppendParameter x500 ($N, never named)")]
    public int Postgresql()
    {
        var builder = new Postgresql.BatchBuilder();
        for (var i = 0; i < _values.Length; i++)
        {
            builder.Append(", ");
            builder.AppendParameter(_values[i]);
        }

        return _values.Length;
    }
}

/// <summary>
///     The parameter-name lookup on its own, isolated from any driver type, at a position inside the
///     512-entry table and at one past the end of it. The second row is what every position used to
///     cost before weasel#558.
/// </summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class ParameterNamesBenchmarks
{
    private const int Count = 500;

    [Benchmark(Baseline = true, Description = "ParameterNames.ForPosition x500 (inside the table)")]
    public string InsideTheTable()
    {
        var last = string.Empty;
        for (var i = 0; i < Count; i++)
        {
            last = Core.ParameterNames.ForPosition(i);
        }

        return last;
    }

    [Benchmark(Description = "ParameterNames.ForPosition x500 (past the table, concatenates)")]
    public string PastTheTable()
    {
        var last = string.Empty;
        for (var i = 0; i < Count; i++)
        {
            last = Core.ParameterNames.ForPosition(1024 + i);
        }

        return last;
    }
}
