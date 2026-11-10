#nullable enable
namespace Weasel.Storage;

/// <summary>
///     The two passes a dirty-tracking session makes over its <see cref="IStorageSession.ChangeTrackers" />
///     on every save: detect what changed before the operations are taken, and re-baseline after a
///     successful commit.
/// </summary>
public static class DirtyTrackingSweep
{
    /// <summary>
    ///     Ask every tracker whether its document changed since it was loaded, and hand each resulting
    ///     operation to <paramref name="enqueue" />.
    /// </summary>
    /// <remarks>
    ///     Run it <em>before</em> the session takes its operations, so a detected change commits in the
    ///     same transaction as everything else. Indexed rather than <c>foreach</c>:
    ///     <see cref="IChangeTracker.DetectChanges" /> resolves storage, which can register a mapping, and
    ///     a selector may append to the same list while it runs.
    /// </remarks>
    /// <returns>How many trackers reported a change.</returns>
    public static int DetectChanges(IList<IChangeTracker> trackers, IStorageSession session,
        Action<IStorageOperation> enqueue)
    {
        ArgumentNullException.ThrowIfNull(trackers);
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(enqueue);

        var changed = 0;
        for (var i = 0; i < trackers.Count; i++)
        {
            if (trackers[i].DetectChanges(session, out var operation))
            {
                enqueue(operation);
                changed++;
            }
        }

        return changed;
    }

    /// <summary>
    ///     After a successful commit, re-baseline every tracker against what was just written, and start
    ///     tracking every document the unit of work stored but had not loaded.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Without the re-baseline, the next save would compare against the document as it was
    ///         <em>read</em> and write it again unchanged. Without the second half, a document created by
    ///         <c>Store</c> rather than loaded would never be dirty-tracked.
    ///     </para>
    ///     <para>
    ///         A document is tracked once however many of <paramref name="written" /> carry it: the set of
    ///         tracked documents is updated as trackers are added, by reference, so storing the same
    ///         instance twice in one unit of work does not register two trackers for it.
    ///     </para>
    ///     <para>
    ///         Call it outside any retry of the write itself: re-baselining inside a retried delegate
    ///         leaves the retry comparing against a snapshot it already took.
    ///     </para>
    /// </remarks>
    /// <param name="trackers">The session's change trackers.</param>
    /// <param name="session">The session the commit ran on.</param>
    /// <param name="written">The operations the commit executed.</param>
    public static void Reset(IList<IChangeTracker> trackers, IStorageSession session,
        IEnumerable<IStorageOperation> written)
    {
        ArgumentNullException.ThrowIfNull(trackers);
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(written);

        var tracked = new HashSet<object>(ReferenceEqualityComparer.Instance);
        for (var i = 0; i < trackers.Count; i++)
        {
            trackers[i].Reset(session);
            tracked.Add(trackers[i].Document);
        }

        foreach (var operation in written)
        {
            if (operation is IDocumentStorageOperation document && tracked.Add(document.Document))
            {
                trackers.Add(document.ToTracker(session));
            }
        }
    }
}
