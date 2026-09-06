#nullable enable
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;

namespace Weasel.Storage;

/// <summary>
/// The standard <see cref="IChangeTracker"/>: snapshots a loaded document's clean JSON and, at
/// save time, detects changes by JSON comparison, producing the upsert/overwrite operation that
/// persists the change.
/// </summary>
public class ChangeTracker<T>: IChangeTracker where T : notnull
{
    private readonly T _document;
    private string _json;

    public ChangeTracker(IStorageSession session, T document)
    {
        _document = document;
        _json = session.Serializer.ToCleanJson(document);
    }

    public object Document => _document;

    public bool DetectChanges(IStorageSession session, [NotNullWhen(true)]out IStorageOperation? operation)
    {
        var newJson = session.Serializer.ToCleanJson(_document);

        // The document is unchanged when it serializes to the text it serialized to on load.
        // By default this ordinal compare is the whole detector — see SemanticallyEqual for the
        // fallback and why it is opt-in.
        if (string.Equals(_json, newJson, StringComparison.Ordinal))
        {
            operation = null;
            return false;
        }

        if (session.UseSemanticJsonChangeDetection && SemanticallyEqual(_json, newJson))
        {
            operation = null;
            return false;
        }

        // newJson is deliberately not handed to the operation to save it a serialization. Two
        // things stand in the way. The data column is written with ToJson, not ToCleanJson, and
        // those differ wherever a serializer carries type metadata (Newtonsoft's clean settings
        // pin TypeNameHandling.None), so reusing the clean text would strip $type out of stored
        // polymorphic documents. And the operation serializes only after its write binders have
        // projected the session's correlation / causation / last-modified-by / headers onto the
        // document, which is later than this.
        //
        // When the session opts out of concurrency checks (Concurrency=Disabled) and the
        // document uses optimistic versioning, route through Overwrite so the SQL drops the
        // WHERE mt_version = ? guard — otherwise a stale version on the dirty-tracking copy
        // turns into a no-op UPDATE and SaveChanges raises a concurrency exception despite the
        // opt-out.
        var storage = session.Database.Providers.StorageFor<T>().DirtyTracking;
        operation = session.Concurrency == ConcurrencyChecks.Disabled
                    && (storage.UseOptimisticConcurrency || storage.UseNumericRevisions)
            ? storage.Overwrite(_document, session, session.TenantId)
            : storage.Upsert(_document, session, session.TenantId);

        return true;
    }

    /// <summary>
    /// The opt-in fallback behind <see cref="IStorageSession.UseSemanticJsonChangeDetection"/>:
    /// parse both documents and compare the trees, so JSON that differs only in property order
    /// (or in whitespace) reports as unchanged.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Off by default because it spends two whole <see cref="JsonNode"/> trees, built and thrown
    /// away, to reach an answer the ordinal compare above reaches for free — measured over a 5 KB
    /// document at roughly 8x the time and 10x the allocation of the unchanged path, and dearer
    /// than detecting a genuine change, because a real change is usually visible early in the
    /// tree while proving equality has to walk all of it (weasel#577).
    /// </para>
    /// <para>
    /// Skipping it cannot cost a write. It is a check that only ever turns "changed" into
    /// "unchanged", so leaving it off can produce a redundant, semantically identical update but
    /// never a missed one — which is the trade the tracker wants, since a false negative silently
    /// drops the user's edit.
    /// </para>
    /// <para>
    /// Not inlined: keeping the <see cref="JsonNode"/> code out of <see cref="DetectChanges"/>
    /// leaves the hot path — one serialize and one string compare — as small as it reads.
    /// </para>
    /// </remarks>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static bool SemanticallyEqual(string json, string newJson)
    {
        return JsonNode.DeepEquals(JsonNode.Parse(json), JsonNode.Parse(newJson));
    }

    /// <summary>
    /// Re-baseline after a successful commit.
    /// </summary>
    /// <remarks>
    /// This deliberately re-serializes rather than reusing the JSON <see cref="DetectChanges"/>
    /// already produced, because the document is mutated in between: the write binders project
    /// the session's correlation / causation / last-modified-by / headers onto the document
    /// before the body is serialized (<see cref="IDocumentMetadataBinder{TDoc}.ApplyToDocument"/>),
    /// and the concurrency operations write the new version or revision back onto it from their
    /// postprocess. A baseline captured before those would report a phantom change on the very
    /// next <see cref="DetectChanges"/>.
    /// </remarks>
    public void Reset(IStorageSession session)
    {
        _json = session.Serializer.ToCleanJson(_document);
    }
}
