using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using Weasel.Core;
using Weasel.Sqlite;
using Weasel.Sqlite.Tables;

namespace Weasel.Benchmarks;

/// <summary>
///     <see cref="SchemaMigration.DetermineAsync(System.Data.Common.DbConnection, Migrator, CancellationToken, ISchemaObject[])" />
///     over 200 tables, against a real SQLite database file.
/// </summary>
/// <remarks>
///     <para>
///         This one is not a pure micro-benchmark — it does real I/O against a temp-file SQLite
///         database — and that is deliberate: the thing weasel#560 changed is how many times each
///         object's <c>ConfigureQueryCommand</c> renders, and rendering is only meaningful next to
///         the round trip it feeds. Pricing used to be a separate probe pass (a throwaway builder
///         per object, running the whole introspection query just to read <c>Parameters.Count</c>
///         off it), so every determination rendered every query twice. The costs are now recorded
///         from the real render.
///     </para>
///     <para>
///         SQLite is the provider here because it needs no container, so the benchmark runs
///         anywhere. The shape of the work — render N queries, execute one batched command, read N
///         result sets, diff N tables — is the same on every provider.
///     </para>
///     <para>
///         <b>MatchingSchema</b> is the steady state a healthy application start hits: every table
///         already exists as configured, so the whole cost is introspection and comparison.
///         <b>EmptyDatabase</b> is the cold start: every table comes back missing.
///     </para>
/// </remarks>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class SchemaMigrationBenchmarks
{
    private const int TableCount = 200;

    private readonly SqliteMigrator _migrator = new();

    private string _populatedPath = null!;
    private string _emptyPath = null!;
    private SqliteConnection _populated = null!;
    private SqliteConnection _empty = null!;
    private Table[] _tables = null!;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _tables = Enumerable.Range(0, TableCount).Select(buildTable).ToArray();

        _populatedPath = Path.Combine(Path.GetTempPath(), "weasel-bench-" + Guid.NewGuid().ToString("N") + ".db");
        _emptyPath = Path.Combine(Path.GetTempPath(), "weasel-bench-" + Guid.NewGuid().ToString("N") + ".db");

        _populated = new SqliteConnection($"Data Source={_populatedPath};");
        await _populated.OpenAsync();

        _empty = new SqliteConnection($"Data Source={_emptyPath};");
        await _empty.OpenAsync();

        // Apply the whole set once, so the "matching schema" run measures a determination that
        // finds no differences.
        var migration = await SchemaMigration
            .DetermineAsync(_populated, _migrator, CancellationToken.None, _tables.Cast<ISchemaObject>().ToArray());
        await _migrator.ApplyAllAsync(_populated, migration, JasperFx.AutoCreate.All);
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        await _populated.DisposeAsync();
        await _empty.DisposeAsync();
        SqliteConnection.ClearAllPools();

        foreach (var path in new[] { _populatedPath, _emptyPath })
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // A benchmark run leaving a temp file behind is not worth failing over.
            }
        }
    }

    [Benchmark(Baseline = true, Description = "DetermineAsync over 200 matching tables")]
    public async Task<int> MatchingSchema()
    {
        var migration = await SchemaMigration
            .DetermineAsync(_populated, _migrator, CancellationToken.None, _tables.Cast<ISchemaObject>().ToArray());
        return migration.Deltas.Count;
    }

    [Benchmark(Description = "DetermineAsync over 200 tables against an empty database")]
    public async Task<int> EmptyDatabase()
    {
        var migration = await SchemaMigration
            .DetermineAsync(_empty, _migrator, CancellationToken.None, _tables.Cast<ISchemaObject>().ToArray());
        return migration.Deltas.Count;
    }

    private static Table buildTable(int index)
    {
        var table = new Table("bench_table_" + index);
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<string>("data");
        table.AddColumn<long>("version");
        table.AddColumn<string>("created_at");
        return table;
    }
}
