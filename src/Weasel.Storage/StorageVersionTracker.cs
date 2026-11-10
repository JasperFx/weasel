#nullable enable
namespace Weasel.Storage;

/// <summary>
///     The default <see cref="IVersionTracker" />: session-scoped, in-memory bookkeeping of the
///     optimistic-concurrency version (a <see cref="Guid" />) or numeric revision (a <see langword="long" />)
///     last seen for each document, keyed by document type and id.
/// </summary>
/// <remarks>
///     <para>
///         Versions and revisions are kept in separate dictionaries, so a document type tracked both
///         ways never trips over the other's id type. Asking for the same document type with a different
///         id type replaces the earlier dictionary rather than throwing.
///     </para>
///     <para>
///         <b>Guarded.</b> A projection daemon can fan concurrent slices out onto one session, and the
///         storages call <see cref="ForType{TDoc,TId}" /> and <see cref="RevisionsFor{TDoc,TId}" /> while
///         <em>constructing</em> an operation, on the caller's thread. Two threads adding to the outer
///         dictionary at once lose an entry silently, so every member reachable from a caller takes the
///         lock. The lock is uncontended in an ordinary session and costs next to nothing there.
///     </para>
///     <para>
///         The inner dictionaries are handed out unguarded, deliberately: they are written by an
///         operation's postprocessing, which a session runs sequentially on one batch.
///     </para>
///     <para>
///         Named with the <c>Storage</c> prefix because Marten carries a public <c>VersionTracker</c>,
///         and a file importing both namespaces would otherwise see CS0104.
///     </para>
/// </remarks>
public class StorageVersionTracker: IVersionTracker
{
    private readonly Dictionary<Type, object> _versions = new();
    private readonly Dictionary<Type, object> _revisions = new();
    private readonly Lock _lock = new();

    /// <inheritdoc />
    public Dictionary<TId, Guid> ForType<TDoc, TId>() where TId : notnull
    {
        lock (_lock)
        {
            if (_versions.TryGetValue(typeof(TDoc), out var raw) && raw is Dictionary<TId, Guid> existing)
            {
                return existing;
            }

            var fresh = new Dictionary<TId, Guid>();
            _versions[typeof(TDoc)] = fresh;
            return fresh;
        }
    }

    /// <inheritdoc />
    public Dictionary<TId, long> RevisionsFor<TDoc, TId>() where TId : notnull
    {
        lock (_lock)
        {
            if (_revisions.TryGetValue(typeof(TDoc), out var raw) && raw is Dictionary<TId, long> existing)
            {
                return existing;
            }

            var fresh = new Dictionary<TId, long>();
            _revisions[typeof(TDoc)] = fresh;
            return fresh;
        }
    }

    /// <inheritdoc />
    public Guid? VersionFor<TDoc, TId>(TId id) where TId : notnull
    {
        lock (_lock)
        {
            return _versions.TryGetValue(typeof(TDoc), out var raw) && raw is Dictionary<TId, Guid> versions
                                                                      && versions.TryGetValue(id, out var version)
                ? version
                : null;
        }
    }

    /// <inheritdoc />
    public long? RevisionFor<TDoc, TId>(TId id) where TId : notnull
    {
        lock (_lock)
        {
            return _revisions.TryGetValue(typeof(TDoc), out var raw) && raw is Dictionary<TId, long> revisions
                                                                       && revisions.TryGetValue(id, out var revision)
                ? revision
                : null;
        }
    }

    /// <inheritdoc />
    public void StoreVersion<TDoc, TId>(TId id, Guid guid) where TId : notnull
    {
        var versions = ForType<TDoc, TId>();

        lock (_lock)
        {
            versions[id] = guid;
        }
    }

    /// <inheritdoc />
    public void StoreRevision<TDoc, TId>(TId id, long revision) where TId : notnull
    {
        var revisions = RevisionsFor<TDoc, TId>();

        lock (_lock)
        {
            revisions[id] = revision;
        }
    }

    /// <inheritdoc />
    public void ClearVersion<TDoc, TId>(TId id) where TId : notnull
    {
        lock (_lock)
        {
            if (_versions.TryGetValue(typeof(TDoc), out var raw) && raw is Dictionary<TId, Guid> versions)
            {
                versions.Remove(id);
            }
        }
    }

    /// <inheritdoc />
    public void ClearRevision<TDoc, TId>(TId id) where TId : notnull
    {
        lock (_lock)
        {
            if (_revisions.TryGetValue(typeof(TDoc), out var raw) && raw is Dictionary<TId, long> revisions)
            {
                revisions.Remove(id);
            }
        }
    }
}
