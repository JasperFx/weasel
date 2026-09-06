using BenchmarkDotNet.Attributes;
using Weasel.Benchmarks.Support;
using Weasel.Core;
using Weasel.Storage;

namespace Weasel.Benchmarks;

/// <summary>
///     <see cref="ChangeTracker{T}.DetectChanges" /> over a ~5 KB document.
/// </summary>
/// <remarks>
///     Dirty tracking runs this once per tracked document on every <c>SaveChanges</c>, so a session
///     that loaded a hundred documents and changed one pays the unchanged cost ninety-nine times.
///     Three paths are worth separating:
///     <list type="bullet">
///         <item>
///             <b>Unchanged</b> — the overwhelmingly common case. Re-serializes the document and
///             compares the two strings ordinally. One full-size string allocation per document.
///         </item>
///         <item>
///             <b>SemanticallyEqual</b> — the strings differ but the JSON means the same thing
///             (a reordered dictionary, most often). Falls through the ordinal compare into two
///             <c>JsonNode.Parse</c> calls and a <c>DeepEquals</c>: two whole object graphs built
///             and thrown away to conclude nothing changed.
///         </item>
///         <item>
///             <b>Changed</b> — pays for the serialize, the failed ordinal compare, the two parses
///             and the failed deep compare before it reaches the storage operation.
///         </item>
///     </list>
/// </remarks>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class ChangeTrackerBenchmarks
{
    private ChangeTracker<BenchmarkDocument> _unchanged = null!;
    private ChangeTracker<BenchmarkDocument> _semanticallyEqual = null!;
    private ChangeTracker<BenchmarkDocument> _changed = null!;
    private BenchmarkDocument _changedDocument = null!;
    private IStorageSession _session = null!;

    /// <summary>The measured JSON size of the reference document, reported by Program.</summary>
    public static int DocumentJsonSize => BenchmarkDocument.JsonSize(BenchmarkDocument.Create());

    [GlobalSetup]
    public void Setup()
    {
        var serializer = StorageSerializerAdapter.For(new SystemTextJsonSerializer());
        _session = new BenchmarkStorageSession(serializer,
            new BenchmarkProviderGraph<BenchmarkDocument>(new NoopDocumentStorage<BenchmarkDocument>()));

        _unchanged = new ChangeTracker<BenchmarkDocument>(_session, BenchmarkDocument.Create());

        // Re-inserting the attribute dictionary in a different order changes the JSON text
        // without changing what the JSON means, which is exactly the case the DeepEquals fallback
        // exists for.
        var reordered = BenchmarkDocument.Create();
        _semanticallyEqual = new ChangeTracker<BenchmarkDocument>(_session, reordered);
        var attributes = reordered.Attributes.ToArray();
        reordered.Attributes.Clear();
        foreach (var pair in attributes.Reverse())
        {
            reordered.Attributes[pair.Key] = pair.Value;
        }

        _changedDocument = BenchmarkDocument.Create();
        _changed = new ChangeTracker<BenchmarkDocument>(_session, _changedDocument);
        _changedDocument.Revision++;
    }

    [Benchmark(Baseline = true, Description = "Unchanged (ordinal string compare wins)")]
    public bool Unchanged()
    {
        return _unchanged.DetectChanges(_session, out _);
    }

    [Benchmark(Description = "Reordered but equal (JsonNode DeepEquals fallback)")]
    public bool SemanticallyEqual()
    {
        return _semanticallyEqual.DetectChanges(_session, out _);
    }

    [Benchmark(Description = "Changed (full detection, then Upsert)")]
    public bool Changed()
    {
        return _changed.DetectChanges(_session, out _);
    }
}
