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
///     Four paths are worth separating:
///     <list type="bullet">
///         <item>
///             <b>Unchanged</b> — the overwhelmingly common case. Re-serializes the document and
///             compares the two strings ordinally. One full-size string allocation per document.
///         </item>
///         <item>
///             <b>SemanticallyEqual</b> — the strings differ but the JSON means the same thing
///             (a reordered dictionary, most often). Since weasel#577 the default answer is "it
///             changed": the ordinal compare misses and the tracker goes straight to the upsert,
///             writing a row of semantically identical JSON.
///         </item>
///         <item>
///             <b>SemanticallyEqualWithFallback</b> — the same document under a session that opted
///             into <see cref="IStorageSession.UseSemanticJsonChangeDetection" />. Falls through
///             the ordinal compare into two <c>JsonNode.Parse</c> calls and a <c>DeepEquals</c>:
///             two whole object graphs built and thrown away to conclude nothing changed. This is
///             what every session paid unconditionally before weasel#577.
///         </item>
///         <item>
///             <b>Changed</b> — pays for the serialize and the failed ordinal compare before it
///             reaches the storage operation.
///         </item>
///     </list>
/// </remarks>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class ChangeTrackerBenchmarks
{
    private ChangeTracker<BenchmarkDocument> _unchanged = null!;
    private ChangeTracker<BenchmarkDocument> _semanticallyEqual = null!;
    private ChangeTracker<BenchmarkDocument> _semanticallyEqualWithFallback = null!;
    private ChangeTracker<BenchmarkDocument> _changed = null!;
    private BenchmarkDocument _changedDocument = null!;
    private IStorageSession _session = null!;
    private IStorageSession _fallbackSession = null!;

    /// <summary>The measured JSON size of the reference document, reported by Program.</summary>
    public static int DocumentJsonSize => BenchmarkDocument.JsonSize(BenchmarkDocument.Create());

    [GlobalSetup]
    public void Setup()
    {
        var serializer = StorageSerializerAdapter.For(new SystemTextJsonSerializer());
        _session = new BenchmarkStorageSession(serializer,
            new BenchmarkProviderGraph<BenchmarkDocument>(new NoopDocumentStorage<BenchmarkDocument>()));
        _fallbackSession = new BenchmarkStorageSession(serializer,
            new BenchmarkProviderGraph<BenchmarkDocument>(new NoopDocumentStorage<BenchmarkDocument>()))
        {
            UseSemanticJsonChangeDetection = true
        };

        _unchanged = new ChangeTracker<BenchmarkDocument>(_session, BenchmarkDocument.Create());

        // Re-inserting the attribute dictionary in a different order changes the JSON text
        // without changing what the JSON means, which is exactly the case the DeepEquals fallback
        // exists for. The tracker has to take its baseline before the reorder, or there is no
        // difference for it to find.
        var reordered = BenchmarkDocument.Create();
        _semanticallyEqual = new ChangeTracker<BenchmarkDocument>(_session, reordered);
        reorderTheAttributes(reordered);

        var reorderedForFallback = BenchmarkDocument.Create();
        _semanticallyEqualWithFallback =
            new ChangeTracker<BenchmarkDocument>(_fallbackSession, reorderedForFallback);
        reorderTheAttributes(reorderedForFallback);

        _changedDocument = BenchmarkDocument.Create();
        _changed = new ChangeTracker<BenchmarkDocument>(_session, _changedDocument);
        _changedDocument.Revision++;

        assertEachScenarioAnswersWhatItClaims();
    }

    /// <summary>
    ///     A tracker whose document was not actually made to differ measures the unchanged path
    ///     under another name, and the table looks plausible while saying nothing. Check the four
    ///     answers before measuring them.
    /// </summary>
    private void assertEachScenarioAnswersWhatItClaims()
    {
        assert(!Unchanged(), "Unchanged should report no change");
        assert(SemanticallyEqual(), "SemanticallyEqual should report a change with the fallback off");
        assert(!SemanticallyEqualWithFallback(), "SemanticallyEqualWithFallback should report no change");
        assert(Changed(), "Changed should report a change");

        static void assert(bool condition, string message)
        {
            if (!condition) throw new InvalidOperationException($"Benchmark setup is wrong: {message}.");
        }
    }

    private static void reorderTheAttributes(BenchmarkDocument document)
    {
        var attributes = document.Attributes.ToArray();
        document.Attributes.Clear();
        foreach (var pair in attributes.Reverse())
        {
            document.Attributes[pair.Key] = pair.Value;
        }
    }

    [Benchmark(Baseline = true, Description = "Unchanged (ordinal string compare wins)")]
    public bool Unchanged()
    {
        return _unchanged.DetectChanges(_session, out _);
    }

    [Benchmark(Description = "Reordered but equal (default: reports a change)")]
    public bool SemanticallyEqual()
    {
        return _semanticallyEqual.DetectChanges(_session, out _);
    }

    [Benchmark(Description = "Reordered but equal (opt-in JsonNode DeepEquals fallback)")]
    public bool SemanticallyEqualWithFallback()
    {
        return _semanticallyEqualWithFallback.DetectChanges(_fallbackSession, out _);
    }

    [Benchmark(Description = "Changed (full detection, then Upsert)")]
    public bool Changed()
    {
        return _changed.DetectChanges(_session, out _);
    }
}
