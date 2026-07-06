#nullable enable
namespace Weasel.Storage;

/// <summary>
///     Registry of <see cref="DocumentProvider{T}"/> instances per document type — the
///     document-provider lookup the closed-shape storage runtime resolves storages through.
/// </summary>
public interface IProviderGraph
{
    DocumentProvider<T> StorageFor<T>() where T : notnull;

    void Append<T>(DocumentProvider<T> provider) where T : notnull;
}
