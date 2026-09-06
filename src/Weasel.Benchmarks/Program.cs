using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;

namespace Weasel.Benchmarks;

public static class Program
{
    /// <summary>
    ///     The checked-in run. Short — a couple of minutes for the whole suite — so that "run the
    ///     benchmarks" is a thing a contributor actually does rather than schedules. The numbers it
    ///     produces are good to a few percent, which is enough to see a change of the size these
    ///     benchmarks exist to catch. For a publishable number use <c>--full</c>; see Results.md.
    /// </summary>
    private static IConfig ShortConfig()
    {
        return DefaultConfig.Instance
            .AddJob(Job.ShortRun
                .WithWarmupCount(3)
                .WithIterationCount(5)
                .WithId("short"));
    }

    public static void Main(string[] args)
    {
        var full = args.Contains("--full", StringComparer.OrdinalIgnoreCase);
        var remaining = args.Where(a => !string.Equals(a, "--full", StringComparison.OrdinalIgnoreCase)).ToArray();

        var config = full ? DefaultConfig.Instance : ShortConfig();

        if (remaining.Length > 0)
        {
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(remaining, config);
            return;
        }

        BenchmarkRunner.Run<ChangeTrackerBenchmarks>(config);
        BenchmarkRunner.Run<BatchBuilderBenchmarks>(config);
        BenchmarkRunner.Run<ParameterNamesBenchmarks>(config);
        BenchmarkRunner.Run<SchemaMigrationBenchmarks>(config);
        BenchmarkRunner.Run<JsonDeserializeBenchmarks>(config);
        BenchmarkRunner.Run<JsonReadBenchmarks>(config);
    }
}
