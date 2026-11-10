#nullable enable
namespace Weasel.Storage;

/// <summary>
///     The thread-safe registry half of an <see cref="IProviderGraph" />: a lock-free read of a
///     published snapshot, and a locked build-and-publish on a miss. The owning store supplies
///     <see cref="BuildProvider{T}" />, which resolves its own mapping for the document type and
///     usually builds the storages with <see cref="ClosedShapeDocumentProviderFactory" />.
/// </summary>
/// <remarks>
///     <para>
///         The snapshot is copy-on-write: a reader never sees a dictionary that is being written to,
///         and a miss is rare once a store is warm, so the copy is paid once per document type.
///     </para>
///     <para>
///         <see cref="BuildProvider{T}" /> runs under the graph's lock and may call
///         <see cref="StorageFor{T}" /> for another type (a subclass resolving its root, say): the lock
///         is re-entrant on the same thread.
///     </para>
/// </remarks>
public abstract class ProviderGraphBase: IProviderGraph
{
    private readonly Lock _lock = new();
    private Dictionary<Type, object> _providers = new();

    public DocumentProvider<T> StorageFor<T>() where T : notnull
    {
        // Read the field once: Append may publish a newer snapshot between two reads.
        var snapshot = _providers;
        if (snapshot.TryGetValue(typeof(T), out var stored))
        {
            return (DocumentProvider<T>)stored;
        }

        lock (_lock)
        {
            if (_providers.TryGetValue(typeof(T), out stored))
            {
                return (DocumentProvider<T>)stored;
            }

            var provider = BuildProvider<T>();
            publish(typeof(T), provider);
            return provider;
        }
    }

    public void Append<T>(DocumentProvider<T> provider) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(provider);

        lock (_lock)
        {
            publish(typeof(T), provider);
        }
    }

    /// <summary>True when a provider for <paramref name="documentType" /> is already registered.</summary>
    public bool HasProviderFor(Type documentType) => _providers.ContainsKey(documentType);

    /// <summary>
    ///     Build the provider for a document type nothing has registered yet. Called once per type,
    ///     under the graph's lock; the result is published for every later caller.
    /// </summary>
    protected abstract DocumentProvider<T> BuildProvider<T>() where T : notnull;

    private void publish(Type documentType, object provider)
    {
        _providers = new Dictionary<Type, object>(_providers) { [documentType] = provider };
    }
}
