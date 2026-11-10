using System.Data.Common;
using JasperFx.MultiTenancy;
using NSubstitute;
using Shouldly;
using Weasel.Core.Identity;
using Weasel.Core.Sequences;
using Weasel.Core.SqlGeneration;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     weasel#740: the closed-shape storage family lifted into Weasel.Storage. The identity-map
///     semantics are Marten's, decided for the issue: a second instance under a mapped id is
///     refused unless the document is <see cref="IEquatable{T}" />, an identity map keyed by another
///     id type is refused, a revision is recorded on Store and zero clears it, and a nested session
///     can share the parent's map.
/// </summary>
public class closed_shape_document_storage_family
{
    private readonly FakeSession theSession = new();

    // ---- the factory picks one leaf per (tracking mode, concurrency mode) ----

    [Theory]
    [InlineData(ClosedShapeTrackingMode.QueryOnly, ConcurrencyMode.Off, "QueryOnlyClosedShapeDocumentStorage`2")]
    [InlineData(ClosedShapeTrackingMode.QueryOnly, ConcurrencyMode.Numeric, "QueryOnlyClosedShapeDocumentStorage`2")]
    [InlineData(ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Off, "UnversionedLightweightClosedShapeDocumentStorage`2")]
    [InlineData(ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Optimistic, "OptimisticLightweightClosedShapeDocumentStorage`2")]
    [InlineData(ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Numeric, "NumericLightweightClosedShapeDocumentStorage`2")]
    [InlineData(ClosedShapeTrackingMode.IdentityMap, ConcurrencyMode.Off, "UnversionedIdentityMapClosedShapeDocumentStorage`2")]
    [InlineData(ClosedShapeTrackingMode.IdentityMap, ConcurrencyMode.Optimistic, "OptimisticIdentityMapClosedShapeDocumentStorage`2")]
    [InlineData(ClosedShapeTrackingMode.IdentityMap, ConcurrencyMode.Numeric, "NumericIdentityMapClosedShapeDocumentStorage`2")]
    [InlineData(ClosedShapeTrackingMode.DirtyTracking, ConcurrencyMode.Off, "UnversionedDirtyTrackingClosedShapeDocumentStorage`2")]
    [InlineData(ClosedShapeTrackingMode.DirtyTracking, ConcurrencyMode.Optimistic, "OptimisticDirtyTrackingClosedShapeDocumentStorage`2")]
    [InlineData(ClosedShapeTrackingMode.DirtyTracking, ConcurrencyMode.Numeric, "NumericDirtyTrackingClosedShapeDocumentStorage`2")]
    public void factory_builds_the_leaf_for_the_mode_pair(ClosedShapeTrackingMode mode, ConcurrencyMode concurrency,
        string expected)
    {
        var storage = ClosedShapeDocumentProviderFactory.BuildStorage(mode, descriptor(concurrency), new FakeRules());

        storage.GetType().Name.ShouldBe(expected);
        storage.TrackingMode.ShouldBe(mode);
        storage.UseOptimisticConcurrency.ShouldBe(concurrency == ConcurrencyMode.Optimistic);
        storage.UseNumericRevisions.ShouldBe(concurrency == ConcurrencyMode.Numeric);
    }

    [Fact]
    public void provider_fills_all_four_slots_with_their_own_mode()
    {
        var provider = ClosedShapeDocumentProviderFactory.Build(descriptor(ConcurrencyMode.Off), new FakeRules());

        ((IClosedShapeDocumentStorage)provider.QueryOnly).TrackingMode.ShouldBe(ClosedShapeTrackingMode.QueryOnly);
        ((IClosedShapeDocumentStorage)provider.Lightweight).TrackingMode.ShouldBe(ClosedShapeTrackingMode.Lightweight);
        ((IClosedShapeDocumentStorage)provider.IdentityMap).TrackingMode.ShouldBe(ClosedShapeTrackingMode.IdentityMap);
        ((IClosedShapeDocumentStorage)provider.DirtyTracking).TrackingMode.ShouldBe(ClosedShapeTrackingMode.DirtyTracking);
    }

    [Fact]
    public void each_mode_reads_with_its_own_sql_and_the_rules_are_reachable()
    {
        var rules = new FakeRules();
        var provider = ClosedShapeDocumentProviderFactory.Build(descriptor(ConcurrencyMode.Off), rules);

        provider.QueryOnly.SelectFields().ShouldBe(["d.data"]);
        provider.Lightweight.SelectFields().ShouldBe(["d.id", "d.data"]);
        ((IClosedShapeDocumentStorage)provider.Lightweight).Rules.ShouldBeSameAs(rules);
    }

    // ---- each leaf reads through its own selector family ----

    [Theory]
    [InlineData(ClosedShapeTrackingMode.QueryOnly, ConcurrencyMode.Optimistic, "FlatClosedShapeQueryOnlySelector`2")]
    [InlineData(ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Off, "FlatUnversionedClosedShapeLightweightSelector`2")]
    [InlineData(ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Optimistic, "FlatOptimisticClosedShapeLightweightSelector`2")]
    [InlineData(ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Numeric, "FlatNumericClosedShapeLightweightSelector`2")]
    [InlineData(ClosedShapeTrackingMode.IdentityMap, ConcurrencyMode.Off, "FlatUnversionedClosedShapeIdentityMapSelector`2")]
    [InlineData(ClosedShapeTrackingMode.IdentityMap, ConcurrencyMode.Optimistic, "FlatOptimisticClosedShapeIdentityMapSelector`2")]
    [InlineData(ClosedShapeTrackingMode.IdentityMap, ConcurrencyMode.Numeric, "FlatNumericClosedShapeIdentityMapSelector`2")]
    [InlineData(ClosedShapeTrackingMode.DirtyTracking, ConcurrencyMode.Off, "FlatUnversionedClosedShapeDirtyTrackingSelector`2")]
    [InlineData(ClosedShapeTrackingMode.DirtyTracking, ConcurrencyMode.Optimistic, "FlatOptimisticClosedShapeDirtyTrackingSelector`2")]
    [InlineData(ClosedShapeTrackingMode.DirtyTracking, ConcurrencyMode.Numeric, "FlatNumericClosedShapeDirtyTrackingSelector`2")]
    public void each_leaf_builds_its_own_selector(ClosedShapeTrackingMode mode, ConcurrencyMode concurrency,
        string expected)
    {
        build(mode, concurrency).BuildSelector(theSession).GetType().Name.ShouldBe(expected);
    }

    [Theory]
    [InlineData(ConcurrencyMode.Optimistic)]
    [InlineData(ConcurrencyMode.Numeric)]
    public void a_shared_projection_session_gets_the_lightweight_selector_that_captures_no_versions(
        ConcurrencyMode concurrency)
    {
        var rules = new FakeRules { SharedProjectionSession = true };

        build(ClosedShapeTrackingMode.Lightweight, concurrency, rules).BuildSelector(theSession)
            .ShouldBeOfType<FlatUnversionedClosedShapeLightweightSelector<Doc, Guid>>();

        // Only Lightweight swaps: the identity-map modes keep their own selector.
        build(ClosedShapeTrackingMode.IdentityMap, concurrency, rules).BuildSelector(theSession)
            .ShouldNotBeOfType<FlatUnversionedClosedShapeLightweightSelector<Doc, Guid>>();
    }

    // ---- identity map: Store ----

    [Theory]
    [InlineData(ClosedShapeTrackingMode.IdentityMap)]
    [InlineData(ClosedShapeTrackingMode.DirtyTracking)]
    public void storing_a_different_instance_under_a_mapped_id_is_refused(ClosedShapeTrackingMode mode)
    {
        var storage = build(mode);
        var id = Guid.NewGuid();
        storage.Store(theSession, new Doc { Id = id });

        Should.Throw<InvalidOperationException>(() => storage.Store(theSession, new Doc { Id = id }));
    }

    [Fact]
    public void storing_the_same_instance_twice_is_fine()
    {
        var storage = build(ClosedShapeTrackingMode.IdentityMap);
        var doc = new Doc { Id = Guid.NewGuid() };

        storage.Store(theSession, doc);
        storage.Store(theSession, doc);

        mapOf<Doc>()[doc.Id].ShouldBeSameAs(doc);
    }

    [Fact]
    public void an_equatable_document_is_taken_at_its_word()
    {
        var storage = ClosedShapeDocumentProviderFactory.BuildStorage(ClosedShapeTrackingMode.IdentityMap,
            descriptor<EquatableDoc>(ConcurrencyMode.Off, d => d.Id), new FakeRules<EquatableDoc>());
        var id = Guid.NewGuid();
        storage.Store(theSession, new EquatableDoc { Id = id });

        var second = new EquatableDoc { Id = id };
        storage.Store(theSession, second);

        mapOf<EquatableDoc>()[id].ShouldBeSameAs(second);
    }

    [Fact]
    public void an_identity_map_keyed_by_another_id_type_is_refused_on_store()
    {
        var storage = build(ClosedShapeTrackingMode.IdentityMap);
        theSession.ItemMap[typeof(Doc)] = new Dictionary<string, Doc>();

        Should.Throw<IdentityMapIdTypeMismatchException>(() => storage.Store(theSession, new Doc { Id = Guid.NewGuid() }));
    }

    [Fact]
    public async Task an_identity_map_keyed_by_another_id_type_is_refused_on_load()
    {
        var storage = build(ClosedShapeTrackingMode.IdentityMap);
        theSession.ItemMap[typeof(Doc)] = new Dictionary<string, Doc>();

        await Should.ThrowAsync<IdentityMapIdTypeMismatchException>(() =>
            storage.LoadAsync(Guid.NewGuid(), theSession, CancellationToken.None));
        await Should.ThrowAsync<IdentityMapIdTypeMismatchException>(() =>
            storage.LoadManyAsync([Guid.NewGuid()], theSession, CancellationToken.None));
    }

    [Fact]
    public void a_lightweight_store_keeps_no_map()
    {
        build(ClosedShapeTrackingMode.Lightweight).Store(theSession, new Doc { Id = Guid.NewGuid() });

        theSession.ItemMap.ShouldBeEmpty();
    }

    [Theory]
    [InlineData(ClosedShapeTrackingMode.Lightweight)]
    [InlineData(ClosedShapeTrackingMode.IdentityMap)]
    public void a_store_marks_the_document_for_storage(ClosedShapeTrackingMode mode)
    {
        var doc = new Doc { Id = Guid.NewGuid() };
        build(mode).Store(theSession, doc);

        theSession.Added.ShouldBe([(doc.Id, (object)doc)]);
    }

    // ---- versions and revisions ----

    [Theory]
    [InlineData(ClosedShapeTrackingMode.Lightweight)]
    [InlineData(ClosedShapeTrackingMode.IdentityMap)]
    public void a_revision_is_recorded_and_zero_clears_it(ClosedShapeTrackingMode mode)
    {
        var storage = build(mode, ConcurrencyMode.Numeric);
        var doc = new Doc { Id = Guid.NewGuid() };

        storage.Store(theSession, doc, 3L);
        theSession.Versions.Received().StoreRevision<Doc, Guid>(doc.Id, 3);

        storage.Eject(theSession, doc);
        storage.Store(theSession, doc, 0L);
        theSession.Versions.Received().ClearRevision<Doc, Guid>(doc.Id);
    }

    [Theory]
    [InlineData(ClosedShapeTrackingMode.Lightweight)]
    [InlineData(ClosedShapeTrackingMode.IdentityMap)]
    public void a_version_is_recorded_and_null_clears_it(ClosedShapeTrackingMode mode)
    {
        var storage = build(mode, ConcurrencyMode.Optimistic);
        var doc = new Doc { Id = Guid.NewGuid() };
        var version = Guid.NewGuid();

        storage.Store(theSession, doc, version);
        theSession.Versions.Received().StoreVersion<Doc, Guid>(doc.Id, version);

        storage.Store(theSession, doc, (Guid?)null);
        theSession.Versions.Received().ClearVersion<Doc, Guid>(doc.Id);
    }

    [Theory]
    [InlineData(ClosedShapeTrackingMode.Lightweight)]
    [InlineData(ClosedShapeTrackingMode.IdentityMap)]
    public void a_store_without_numeric_revisions_refuses_one(ClosedShapeTrackingMode mode)
    {
        var rules = new FakeRules { NumericRevisionsNotSupportedReason = "no revisions here" };
        var storage = ClosedShapeDocumentProviderFactory.BuildStorage(mode, descriptor(ConcurrencyMode.Off), rules);

        Should.Throw<NotSupportedException>(() => storage.Store(theSession, new Doc { Id = Guid.NewGuid() }, 2L))
            .Message.ShouldBe("no revisions here");
    }

    // ---- loads ----

    [Fact]
    public async Task load_answers_from_the_map_without_a_round_trip()
    {
        var storage = build(ClosedShapeTrackingMode.IdentityMap);
        var doc = new Doc { Id = Guid.NewGuid() };
        storage.Store(theSession, doc);

        (await storage.LoadAsync(doc.Id, theSession, CancellationToken.None)).ShouldBeSameAs(doc);
        theSession.Reads.ShouldBe(0);
    }

    [Fact]
    public async Task load_many_with_every_id_mapped_makes_no_round_trip()
    {
        var storage = build(ClosedShapeTrackingMode.IdentityMap);
        var first = new Doc { Id = Guid.NewGuid() };
        var second = new Doc { Id = Guid.NewGuid() };
        storage.Store(theSession, first);
        storage.Store(theSession, second);

        var loaded = await storage.LoadManyAsync([second.Id, first.Id], theSession, CancellationToken.None);

        loaded.ShouldBe([second, first]);
        theSession.Reads.ShouldBe(0);
    }

    [Fact]
    public async Task query_only_refuses_writes_and_projection_loads()
    {
        var storage = build(ClosedShapeTrackingMode.QueryOnly);
        var doc = new Doc { Id = Guid.NewGuid() };

        Should.Throw<NotSupportedException>(() => storage.Upsert(doc, theSession, "t"));
        Should.Throw<NotSupportedException>(() => storage.InsertProjected(doc, "t"));
        await Should.ThrowAsync<NotSupportedException>(() =>
            storage.LoadProjectedAsync(doc.Id, Substitute.For<IStorageDatabase>(), "t", CancellationToken.None));

        storage.Store(theSession, doc);
        theSession.ItemMap.ShouldBeEmpty();
        theSession.Added.ShouldBeEmpty();
    }

    // ---- eject ----

    [Fact]
    public void eject_and_eject_by_id_drop_the_document_from_the_map()
    {
        var storage = build(ClosedShapeTrackingMode.IdentityMap);
        var first = new Doc { Id = Guid.NewGuid() };
        var second = new Doc { Id = Guid.NewGuid() };
        storage.Store(theSession, first);
        storage.Store(theSession, second);

        storage.Eject(theSession, first);
        storage.EjectById(theSession, second.Id);

        mapOf<Doc>().ShouldBeEmpty();
    }

    [Fact]
    public void remove_dirty_tracker_drops_only_that_documents_tracker()
    {
        var storage = build(ClosedShapeTrackingMode.DirtyTracking);
        var kept = new Doc { Id = Guid.NewGuid() };
        var removed = new Doc { Id = Guid.NewGuid() };
        theSession.ChangeTrackers.Add(tracker(kept));
        theSession.ChangeTrackers.Add(tracker(removed));

        storage.RemoveDirtyTracker(theSession, removed.Id);

        theSession.ChangeTrackers.Single().Document.ShouldBeSameAs(kept);
    }

    // ---- nested sessions ----

    [Fact]
    public void a_nested_session_shares_the_parents_identity_map_and_versions()
    {
        var rules = new FakeRules();
        var storage = ClosedShapeDocumentProviderFactory.BuildStorage(ClosedShapeTrackingMode.IdentityMap,
            descriptor(ConcurrencyMode.Optimistic), rules);
        var nested = new FakeSession();
        var doc = new Doc { Id = Guid.NewGuid() };

        storage.ShareTenantNeutralStateWith(theSession, nested);
        storage.Store(nested, doc);

        mapOf<Doc>()[doc.Id].ShouldBeSameAs(doc);
        rules.SharedVersions.ShouldBe(1);
    }

    [Fact]
    public void an_unversioned_nested_session_shares_no_versions_and_lightweight_shares_nothing()
    {
        var rules = new FakeRules();
        var nested = new FakeSession();

        ClosedShapeDocumentProviderFactory.BuildStorage(ClosedShapeTrackingMode.IdentityMap, descriptor(ConcurrencyMode.Off), rules)
            .ShareTenantNeutralStateWith(theSession, nested);
        rules.SharedVersions.ShouldBe(0);
        nested.ItemMap.ShouldContainKey(typeof(Doc));

        var other = new FakeSession();
        ClosedShapeDocumentProviderFactory.BuildStorage(ClosedShapeTrackingMode.Lightweight, descriptor(ConcurrencyMode.Optimistic), rules)
            .ShareTenantNeutralStateWith(theSession, other);
        other.ItemMap.ShouldBeEmpty();
    }

    // ---- writes ----

    [Fact]
    public void writes_run_over_the_descriptor_the_rules_choose()
    {
        var rules = new FakeRules();
        var storage = build(ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Off, rules);
        var doc = new Doc { Id = Guid.NewGuid() };

        storage.Upsert(doc, theSession, "t").ShouldBeOfType<UnversionedClosedShapeUpsertOperation<Doc, Guid>>();
        storage.UpsertProjected(doc, "t");

        rules.WriteRequests.ShouldBe([(OperationRole.Upsert, true), (OperationRole.Upsert, false)]);
    }

    [Theory]
    [InlineData(ConcurrencyMode.Off, typeof(UnversionedClosedShapeInsertOperation<Doc, Guid>))]
    [InlineData(ConcurrencyMode.Optimistic, typeof(OptimisticClosedShapeInsertOperation<Doc, Guid>))]
    [InlineData(ConcurrencyMode.Numeric, typeof(NumericClosedShapeInsertOperation<Doc, Guid>))]
    public void insert_builds_the_operation_for_the_concurrency_mode(ConcurrencyMode concurrency, Type expected)
    {
        foreach (var mode in new[]
                 {
                     ClosedShapeTrackingMode.Lightweight, ClosedShapeTrackingMode.IdentityMap,
                     ClosedShapeTrackingMode.DirtyTracking
                 })
        {
            build(mode, concurrency).Insert(new Doc { Id = Guid.NewGuid() }, theSession, "t").GetType().ShouldBe(expected);
        }
    }

    // ---- provider graph ----

    [Fact]
    public void provider_graph_builds_once_and_honours_append()
    {
        var graph = new CountingGraph();

        var first = graph.StorageFor<Doc>();
        graph.StorageFor<Doc>().ShouldBeSameAs(first);
        graph.Built.ShouldBe(1);

        var appended = ClosedShapeDocumentProviderFactory.Build(descriptor(ConcurrencyMode.Off), new FakeRules());
        graph.Append(appended);
        graph.StorageFor<Doc>().ShouldBeSameAs(appended);
        graph.HasProviderFor(typeof(Doc)).ShouldBeTrue();
        graph.HasProviderFor(typeof(EquatableDoc)).ShouldBeFalse();
    }

    // ---- helpers ----

    private IClosedShapeDocumentStorage<Doc, Guid> build(ClosedShapeTrackingMode mode,
        ConcurrencyMode concurrency = ConcurrencyMode.Off, FakeRules? rules = null)
        => ClosedShapeDocumentProviderFactory.BuildStorage(mode, descriptor(concurrency), rules ?? new FakeRules());

    private Dictionary<Guid, T> mapOf<T>() => (Dictionary<Guid, T>)theSession.ItemMap[typeof(T)];

    private static IChangeTracker tracker(Doc doc)
    {
        var tracker = Substitute.For<IChangeTracker>();
        tracker.Document.Returns(doc);
        return tracker;
    }

    private static DocumentStorageDescriptor<Doc, Guid> descriptor(ConcurrencyMode concurrency)
        => descriptor<Doc>(concurrency, d => d.Id);

    private static DocumentStorageDescriptor<T, Guid> descriptor<T>(ConcurrencyMode concurrency, Func<T, Guid> id)
        where T : notnull
        => new(new GuidIdentification<T>(id), Substitute.For<IStorageSerializer>(), Substitute.For<IStorageDialect>(),
            [], [], [], [], "upsert", "insert", "update", "overwrite", false, concurrency, null, null, -1, null, -1,
            "docs");

    public class Doc
    {
        public Guid Id { get; set; }
    }

    public class EquatableDoc: IEquatable<EquatableDoc>
    {
        public Guid Id { get; set; }
        public bool Equals(EquatableDoc? other) => other is not null && other.Id == Id;
        public override bool Equals(object? obj) => Equals(obj as EquatableDoc);
        public override int GetHashCode() => Id.GetHashCode();
    }

    private sealed class GuidIdentification<T>(Func<T, Guid> id): IIdentification<T, Guid>
    {
        public Guid Identity(T document) => id(document);
        public Guid AssignIfMissing(T document, ISequenceSource sequences) => id(document);
    }

    private sealed class CountingGraph: ProviderGraphBase
    {
        public int Built { get; private set; }

        protected override DocumentProvider<T> BuildProvider<T>()
        {
            Built++;
            return (DocumentProvider<T>)(object)ClosedShapeDocumentProviderFactory.Build(descriptor(ConcurrencyMode.Off),
                new FakeRules());
        }
    }

    private class FakeRules: FakeRules<Doc>;

    private class FakeRules<T>: IClosedShapeStorageRules<T, Guid> where T : notnull
    {
        public int SharedVersions { get; private set; }
        public List<(OperationRole, bool)> WriteRequests { get; } = [];
        public string? NumericRevisionsNotSupportedReason { get; init; }
        public bool SharedProjectionSession { get; init; }

        public bool IsSharedProjectionSession(IStorageSession session) => SharedProjectionSession;

        public DbObjectName TableName { get; } = new("public", "docs");
        public TenancyStyle TenancyStyle => TenancyStyle.Single;
        public IReadOnlyList<IDuplicatedField> DuplicatedFields => [];
        public string TruncateSql => "truncate docs";

        public ClosedShapeReadSql ReadSqlFor(ClosedShapeTrackingMode mode)
        {
            string[] fields = mode == ClosedShapeTrackingMode.QueryOnly ? ["d.data"] : ["d.id", "d.data"];
            var select = $"select {string.Join(", ", fields)} from docs as d";
            return new ClosedShapeReadSql(fields, select, select + " where id = ?", select + " where id = any(?)");
        }

        public IOperationFragment BuildDeleteFragment(IDocumentStorage<T, Guid> storage)
            => Substitute.For<IOperationFragment>();

        public IOperationFragment BuildHardDeleteFragment(IDocumentStorage<T, Guid> storage)
            => Substitute.For<IOperationFragment>();

        public IDeletion BuildDeletion(IDocumentStorage<T, Guid> storage, IOperationFragment fragment, Guid id,
            ISqlFragment byIdFilter, string tenantId)
            => Substitute.For<IDeletion>();

        public ISqlFragment FilterDocuments(ISqlFragment query, IStorageSession session) => query;
        public ISqlFragment? DefaultWhereFragment() => null;
        public void SetIdentity(T document, Guid identity) => throw new NotSupportedException();

        public DocumentStorageDescriptor<T, Guid> WriteDescriptorFor(OperationRole role, T document,
            IStorageSession? session, DocumentStorageDescriptor<T, Guid> descriptor)
        {
            WriteRequests.Add((role, session is not null));
            return descriptor;
        }

        public void ShareVersions(IStorageSession parent, IStorageSession nested) => SharedVersions++;
    }

    /// <summary>A session with a real item map, change-tracker list, and version-tracker substitute.</summary>
    private sealed class FakeSession: IStorageSession
    {
        public List<(object, object)> Added { get; } = [];
        public int Reads { get; private set; }

        public IStorageSerializer Serializer { get; } = Substitute.For<IStorageSerializer>();
        public IStorageDatabase Database { get; } = Substitute.For<IStorageDatabase>();
        public IVersionTracker Versions { get; } = Substitute.For<IVersionTracker>();
        public IList<IChangeTracker> ChangeTrackers { get; } = new List<IChangeTracker>();
        public Dictionary<Type, object> ItemMap { get; } = new();
        public ConcurrencyChecks Concurrency => ConcurrencyChecks.Enabled;
        public string TenantId { get; set; } = "*DEFAULT*";
        public string? CausationId { get; set; }
        public string? CorrelationId { get; set; }
        public string? CurrentUserName { get; set; }
        public Dictionary<string, object>? Headers => null;
        public bool CausationIdEnabled => false;
        public bool CorrelationIdEnabled => false;
        public bool HeadersEnabled => false;
        public bool UserNameEnabled => false;

        public IDocumentStorage StorageFor(Type documentType) => throw new NotSupportedException();
        public IDocumentStorage<T> StorageFor<T>() where T : notnull => throw new NotSupportedException();
        public void MarkAsAddedForStorage(object id, object document) => Added.Add((id, document));
        public void MarkAsDocumentLoaded(object id, object document) { }

        public Task<DbDataReader> ExecuteReaderAsync(DbCommand command, CancellationToken token = default)
        {
            Reads++;
            throw new InvalidOperationException("No database behind this session.");
        }

        public string NextTempTableName() => "temp1";
    }
}
