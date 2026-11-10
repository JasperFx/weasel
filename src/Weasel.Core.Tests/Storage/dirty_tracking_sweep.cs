using NSubstitute;
using Shouldly;
using Weasel.Core;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests.StorageHelpers;

using IStorageOperation = Weasel.Storage.IStorageOperation;

public class dirty_tracking_sweep
{
    private readonly IStorageSession theSession = Substitute.For<IStorageSession>();

    public class Doc;

    private static IChangeTracker tracker(IStorageOperation? change)
    {
        var tracker = FakeTracker.For(new Doc());
        tracker.DetectChanges(Arg.Any<IStorageSession>(), out Arg.Any<IStorageOperation?>())
            .Returns(call =>
            {
                call[1] = change;
                return change is not null;
            });
        return tracker;
    }

    [Fact]
    public void only_changed_documents_are_enqueued()
    {
        var change = new FakeOperation(OperationRole.Upsert);
        var trackers = new List<IChangeTracker> { tracker(null), tracker(change), tracker(null) };
        var queued = new List<IStorageOperation>();

        DirtyTrackingSweep.DetectChanges(trackers, theSession, queued.Add).ShouldBe(1);

        queued.ShouldBe([change]);
    }

    [Fact]
    public void a_tracker_added_while_detecting_is_swept_too()
    {
        // DetectChanges can resolve storage, and a selector can append to the same list meanwhile.
        var late = new FakeOperation(OperationRole.Upsert);
        var lateTracker = tracker(late);
        var trackers = new List<IChangeTracker>();
        var first = FakeTracker.For(new Doc());
        first.DetectChanges(Arg.Any<IStorageSession>(), out Arg.Any<IStorageOperation?>())
            .Returns(_ =>
            {
                trackers.Add(lateTracker);
                return false;
            });
        trackers.Add(first);
        var queued = new List<IStorageOperation>();

        DirtyTrackingSweep.DetectChanges(trackers, theSession, queued.Add);

        queued.ShouldBe([late]);
    }

    [Fact]
    public void reset_rebaselines_every_tracker()
    {
        var trackers = new List<IChangeTracker> { tracker(null), tracker(null) };

        DirtyTrackingSweep.Reset(trackers, theSession, []);

        foreach (var t in trackers)
        {
            t.Received(1).Reset(theSession);
        }
    }

    [Fact]
    public void reset_starts_tracking_a_stored_document_once()
    {
        var loaded = tracker(null);
        var stored = new Doc();
        var trackers = new List<IChangeTracker> { loaded };

        DirtyTrackingSweep.Reset(trackers, theSession, [
            new FakeDocumentOperation(loaded.Document, OperationRole.Update),
            new FakeDocumentOperation(stored, OperationRole.Insert),
            // The same instance stored twice in one unit of work must not register two trackers.
            new FakeDocumentOperation(stored, OperationRole.Update),
            new FakeOperation(OperationRole.Other)
        ]);

        trackers.Count.ShouldBe(2);
        trackers[1].Document.ShouldBeSameAs(stored);
    }
}
