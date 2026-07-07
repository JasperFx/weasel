#nullable enable
namespace Weasel.Storage;

/// <summary>
///     A storage operation that persists one document instance — surfaces the document for
///     unit-of-work bookkeeping (dedupe on re-Store, change-set reporting) and can convert
///     itself into the dirty-tracking <see cref="IChangeTracker"/> registered after a commit.
/// </summary>
public interface IDocumentStorageOperation: IStorageOperation
{
    object Document { get; }
    IChangeTracker ToTracker(IStorageSession session);
}

/// <summary>
///     Implemented by write operations under numeric-revision concurrency: the caller-supplied
///     revision (0 = auto-increment) and the opt-out that turns a revision miss into a no-op
///     instead of a concurrency exception.
/// </summary>
public interface IRevisionedOperation
{
    long Revision { get; set; }
    bool IgnoreConcurrencyViolation { get; set; }
}

/// <summary>
///     Implemented by storage operations that carry a typed document id, so the session's
///     unit-of-work can dedupe pending operations when a caller issues Store-then-Delete on the
///     same id.
/// </summary>
public interface IIdentifiedOperation<TDoc, TId> where TDoc : notnull where TId : notnull
{
    TId Id { get; }
}
