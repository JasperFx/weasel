#nullable enable
using System.Diagnostics.CodeAnalysis;

namespace Weasel.Storage;

/// <summary>
///     Dirty-tracking seam: watches one loaded document and, at save time, detects whether it
///     changed since load (producing the storage operation that persists the change) and resets
///     its baseline after a successful commit.
/// </summary>
public interface IChangeTracker
{
    object Document { get; }
    bool DetectChanges(IStorageSession session, [NotNullWhen(true)]out IStorageOperation? operation);
    void Reset(IStorageSession session);
}
