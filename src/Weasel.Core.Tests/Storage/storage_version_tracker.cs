using Shouldly;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests.StorageHelpers;

public class storage_version_tracker
{
    private readonly StorageVersionTracker theTracker = new();

    public class Doc;

    public class Other;

    [Fact]
    public void versions_round_trip_and_clear()
    {
        var id = Guid.NewGuid();
        var version = Guid.NewGuid();

        theTracker.VersionFor<Doc, Guid>(id).ShouldBeNull();

        theTracker.StoreVersion<Doc, Guid>(id, version);
        theTracker.VersionFor<Doc, Guid>(id).ShouldBe(version);
        theTracker.ForType<Doc, Guid>()[id].ShouldBe(version);

        theTracker.ClearVersion<Doc, Guid>(id);
        theTracker.VersionFor<Doc, Guid>(id).ShouldBeNull();
    }

    [Fact]
    public void revisions_round_trip_and_clear()
    {
        theTracker.RevisionFor<Doc, string>("a").ShouldBeNull();

        theTracker.StoreRevision<Doc, string>("a", 3);
        theTracker.RevisionFor<Doc, string>("a").ShouldBe(3);
        theTracker.RevisionsFor<Doc, string>()["a"].ShouldBe(3);

        theTracker.ClearRevision<Doc, string>("a");
        theTracker.RevisionFor<Doc, string>("a").ShouldBeNull();
    }

    [Fact]
    public void versions_and_revisions_of_one_type_do_not_collide()
    {
        // The two are kept apart, so a type tracked both ways with different id types never sees the
        // other's dictionary.
        theTracker.StoreVersion<Doc, Guid>(Guid.Empty, Guid.NewGuid());
        theTracker.StoreRevision<Doc, int>(1, 7);

        theTracker.RevisionFor<Doc, int>(1).ShouldBe(7);
        theTracker.VersionFor<Doc, Guid>(Guid.Empty).ShouldNotBeNull();
    }

    [Fact]
    public void the_dictionary_handed_out_is_the_one_tracked()
    {
        // Operations are handed the dictionary at construction and write into it on postprocess.
        var dictionary = theTracker.ForType<Doc, int>();
        dictionary[5] = Guid.Empty;

        theTracker.VersionFor<Doc, int>(5).ShouldBe(Guid.Empty);
        theTracker.ForType<Doc, int>().ShouldBeSameAs(dictionary);
    }

    [Fact]
    public void reading_an_untracked_type_registers_nothing()
    {
        theTracker.VersionFor<Other, int>(1).ShouldBeNull();
        theTracker.ClearVersion<Other, int>(1);

        // Still a fresh dictionary, and the first one created.
        theTracker.ForType<Other, int>().ShouldBeEmpty();
    }

    [Fact]
    public async Task concurrent_writers_on_different_types_lose_nothing()
    {
        // The daemon shape: concurrent slices sharing one session, each writing its own type.
        const int perType = 2_000;
        var start = new TaskCompletionSource();

        Task writer<T>() => Task.Run(async () =>
        {
            await start.Task;
            for (var i = 0; i < perType; i++)
            {
                theTracker.StoreRevision<T, int>(i, i);
                theTracker.StoreVersion<T, int>(i, Guid.Empty);
            }
        });

        var writers = new[] { writer<Doc>(), writer<Other>(), writer<string>(), writer<storage_version_tracker>() };
        start.SetResult();
        await Task.WhenAll(writers);

        theTracker.RevisionsFor<Doc, int>().Count.ShouldBe(perType);
        theTracker.RevisionsFor<Other, int>().Count.ShouldBe(perType);
        theTracker.ForType<string, int>().Count.ShouldBe(perType);
        theTracker.ForType<storage_version_tracker, int>().Count.ShouldBe(perType);
    }
}
