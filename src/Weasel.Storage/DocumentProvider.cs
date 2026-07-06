#nullable enable
namespace Weasel.Storage;

/// <summary>
///     Bundles the four closed-shape storage variants for one document type. A store derives
///     from this to attach store-specific services (e.g. a bulk loader) that are not part of the
///     dialect-neutral contract.
/// </summary>
public class DocumentProvider<T> where T : notnull
{
    public DocumentProvider(IDocumentStorage<T> queryOnly, IDocumentStorage<T> lightweight,
        IDocumentStorage<T> identityMap, IDocumentStorage<T> dirtyTracking)
    {
        QueryOnly = queryOnly;
        Lightweight = lightweight;
        IdentityMap = identityMap;
        DirtyTracking = dirtyTracking;
    }

    public IDocumentStorage<T> QueryOnly { get; }
    public IDocumentStorage<T> Lightweight { get; }
    public IDocumentStorage<T> IdentityMap { get; }
    public IDocumentStorage<T> DirtyTracking { get; }
}
