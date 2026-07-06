#nullable enable
using System;
using System.Collections.Generic;

namespace Weasel.Storage;

/// <summary>
///     Dialect-neutral optimistic-concurrency version/revision tracker seam consumed by the shared
///     document-storage runtime. Every member is database-neutral; the concrete implementation lives
///     in the consuming library (e.g. Marten's <c>VersionTracker</c>).
/// </summary>
public interface IVersionTracker
{
    Dictionary<TId, long> RevisionsFor<TDoc, TId>() where TId : notnull;

    Dictionary<TId, Guid> ForType<TDoc, TId>() where TId : notnull;

    Guid? VersionFor<TDoc, TId>(TId id) where TId : notnull;

    long? RevisionFor<TDoc, TId>(TId id) where TId : notnull;

    void StoreVersion<TDoc, TId>(TId id, Guid guid) where TId : notnull;

    void StoreRevision<TDoc, TId>(TId id, long revision) where TId : notnull;

    void ClearVersion<TDoc, TId>(TId id) where TId : notnull;

    void ClearRevision<TDoc, TId>(TId id) where TId : notnull;
}
