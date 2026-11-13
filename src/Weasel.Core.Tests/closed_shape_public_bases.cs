using System.Reflection;
using NSubstitute;
using Shouldly;
using Weasel.Core.SqlGeneration;
using Weasel.Storage;
using Xunit;
using static Weasel.Core.Tests.closed_shape_document_storage_family;

namespace Weasel.Core.Tests;

/// <summary>
///     weasel#811: the closed-shape tracking-mode bases are public so Marten, Polecat and Fisher can
///     derive them, and the rules seam carries what each store kept outside the family: undelete and
///     restore (Fisher, Polecat), the write half of mapped version members (Polecat), and hierarchy
///     sub-class wrapping (all three). This test project is outside Weasel.Storage, so every subclass
///     here is the shape a store would write.
/// </summary>
public class closed_shape_public_bases
{
    private readonly FakeSession theSession = new();

    // ---- a store derives a tracking-mode base and overrides every seam point ----

    [Fact]
    public async Task every_overridable_member_of_a_derived_storage_is_reached_through_the_interface()
    {
        var storage = new HookedLightweight(descriptor(ConcurrencyMode.Optimistic), new FakeRules<Doc>());
        IDocumentStorage<Doc, Guid> asInterface = storage;
        var doc = new Doc { Id = Guid.NewGuid() };

        asInterface.Insert(doc, theSession, "t").ShouldBeOfType<OptimisticClosedShapeInsertOperation<Doc, Guid>>();
        asInterface.Update(doc, theSession, "t").ShouldBeOfType<OptimisticClosedShapeUpdateOperation<Doc, Guid>>();
        asInterface.Upsert(doc, theSession, "t").ShouldBeOfType<OptimisticClosedShapeUpsertOperation<Doc, Guid>>();
        asInterface.Overwrite(doc, theSession, "t").ShouldBeOfType<OptimisticClosedShapeOverwriteOperation<Doc, Guid>>();
        asInterface.InsertProjected(doc, "t").ShouldBeOfType<OptimisticClosedShapeInsertOperation<Doc, Guid>>();
        asInterface.UpdateProjected(doc, "t").ShouldBeOfType<OptimisticClosedShapeUpdateOperation<Doc, Guid>>();
        asInterface.UpsertProjected(doc, "t").ShouldBeOfType<OptimisticClosedShapeUpsertOperation<Doc, Guid>>();
        asInterface.OverwriteProjected(doc, "t").ShouldBeOfType<OptimisticClosedShapeOverwriteOperation<Doc, Guid>>();
        asInterface.BuildSelector(theSession).ShouldBeOfType<FlatOptimisticClosedShapeLightweightSelector<Doc, Guid>>();
        (await asInterface.LoadProjectedAsync(doc.Id, Substitute.For<IStorageDatabase>(), "t", CancellationToken.None))
            .ShouldBeSameAs(storage.Projected);
        (await asInterface.LoadManyProjectedAsync([], Substitute.For<IStorageDatabase>(), "t", CancellationToken.None))
            .ShouldBeEmpty();
        ((IClosedShapeDocumentStorage)storage).ShareTenantNeutralStateWith(theSession, new FakeSession());

        storage.Hits.ShouldBe([
            "Insert", "Update", "Upsert", "Overwrite", "InsertProjected", "UpdateProjected", "UpsertProjected",
            "OverwriteProjected", "BuildSelector", "LoadProjectedAsync", "LoadManyProjectedAsync",
            "ShareTenantNeutralStateWith"
        ]);
    }

    [Fact]
    public void a_store_derives_all_four_modes_with_nothing_but_constructors_and_keeps_its_own_interface()
    {
        var rules = new FakeRules<Doc>();
        var descriptor = closed_shape_public_bases.descriptor(ConcurrencyMode.Numeric);
        var provider = new DocumentProvider<Doc>(
            new StoreQueryOnly(descriptor, rules),
            new StoreLightweight(descriptor, rules),
            new StoreIdentityMap(descriptor, rules),
            new StoreDirtyTracking(descriptor, rules));

        IDocumentStorage<Doc>[] slots = [provider.QueryOnly, provider.Lightweight, provider.IdentityMap, provider.DirtyTracking];
        slots.ShouldAllBe(x => x is IStoreOwnStorage);
        slots.Cast<IClosedShapeDocumentStorage>().Select(x => x.TrackingMode).ShouldBe([
            ClosedShapeTrackingMode.QueryOnly, ClosedShapeTrackingMode.Lightweight, ClosedShapeTrackingMode.IdentityMap,
            ClosedShapeTrackingMode.DirtyTracking
        ]);
        slots.Cast<IClosedShapeDocumentStorage>().ShouldAllBe(x => ReferenceEquals(x.Rules, rules));
    }

    [Fact]
    public void a_derived_query_only_storage_still_refuses_writes()
    {
        var storage = new StoreQueryOnly(descriptor(ConcurrencyMode.Off), new FakeRules<Doc>());

        Should.Throw<NotSupportedException>(() => storage.Upsert(new Doc(), theSession, "t"));
        storage.BuildSelector(theSession).ShouldBeOfType<FlatClosedShapeQueryOnlySelector<Doc, Guid>>();
    }

    [Fact]
    public void a_derived_identity_map_storage_keeps_the_identity_map_semantics()
    {
        var storage = new StoreIdentityMap(descriptor(ConcurrencyMode.Off), new FakeRules<Doc>());
        var id = Guid.NewGuid();
        storage.Store(theSession, new Doc { Id = id });

        Should.Throw<InvalidOperationException>(() => storage.Store(theSession, new Doc { Id = id }));
    }

    // ---- the defaults a derived storage reaches are what the factory's leaves build ----

    [Theory]
    [InlineData(ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Off, false)]
    [InlineData(ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Optimistic, false)]
    [InlineData(ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Numeric, false)]
    [InlineData(ClosedShapeTrackingMode.IdentityMap, ConcurrencyMode.Off, false)]
    [InlineData(ClosedShapeTrackingMode.IdentityMap, ConcurrencyMode.Optimistic, false)]
    [InlineData(ClosedShapeTrackingMode.IdentityMap, ConcurrencyMode.Numeric, false)]
    [InlineData(ClosedShapeTrackingMode.DirtyTracking, ConcurrencyMode.Off, false)]
    [InlineData(ClosedShapeTrackingMode.DirtyTracking, ConcurrencyMode.Optimistic, false)]
    [InlineData(ClosedShapeTrackingMode.DirtyTracking, ConcurrencyMode.Numeric, false)]
    [InlineData(ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Optimistic, true)]
    [InlineData(ClosedShapeTrackingMode.IdentityMap, ConcurrencyMode.Numeric, true)]
    [InlineData(ClosedShapeTrackingMode.DirtyTracking, ConcurrencyMode.Off, true)]
    public void a_derived_storage_builds_what_the_factory_leaf_builds(ClosedShapeTrackingMode mode,
        ConcurrencyMode concurrency, bool hierarchical)
    {
        var descriptor = closed_shape_public_bases.descriptor(concurrency, hierarchical);
        var leafRules = new FakeRules<Doc>();
        var derivedRules = new FakeRules<Doc>();

        var leaf = ClosedShapeDocumentProviderFactory.BuildStorage(mode, descriptor, leafRules);
        IDocumentStorage<Doc, Guid> derived = mode switch
        {
            ClosedShapeTrackingMode.Lightweight => new StoreLightweight(descriptor, derivedRules),
            ClosedShapeTrackingMode.IdentityMap => new StoreIdentityMap(descriptor, derivedRules),
            _ => new StoreDirtyTracking(descriptor, derivedRules)
        };

        derived.GetType().ShouldNotBe(leaf.GetType());
        derived.BuildSelector(theSession).GetType().ShouldBe(leaf.BuildSelector(theSession).GetType());
        derived.SelectFields().ShouldBe(leaf.SelectFields());

        var doc = new Doc { Id = Guid.NewGuid() };
        writesOf(derived, doc).ShouldBe(writesOf(leaf, doc));
        derivedRules.WriteRequests.ShouldBe(leafRules.WriteRequests);
    }

    [Fact]
    public void a_derived_lightweight_storage_hands_a_shared_projection_session_the_versionless_selector()
    {
        var rules = new FakeRules<Doc> { SharedProjectionSession = true };

        new StoreLightweight(descriptor(ConcurrencyMode.Numeric), rules).BuildSelector(theSession)
            .ShouldBeOfType<FlatUnversionedClosedShapeLightweightSelector<Doc, Guid>>();
    }

    // ---- undelete and restore: the rules object answers (Fisher's and Polecat's shape) ----

    [Fact]
    public void undelete_and_restore_come_from_the_rules_object()
    {
        var rules = new SoftDeletingRules();
        var storage = new StoreLightweight(descriptor(ConcurrencyMode.Optimistic), rules);
        var id = Guid.NewGuid();

        storage.UndeleteFragment.ShouldBeSameAs(rules.UndeleteFragment);
        ((IClosedShapeDocumentStorage)storage).UndeleteFragment.ShouldBeSameAs(rules.UndeleteFragment);

        var undelete = storage.UndeleteForId(id, "red").ShouldBeOfType<RecordedOperation>();
        undelete.Kind.ShouldBe("undelete");
        undelete.Fragment.ShouldBeSameAs(rules.UndeleteFragment);
        undelete.Id.ShouldBe(id);
        undelete.TenantId.ShouldBe("red");

        // Restore is its own operation: it promises a new version, which an undelete does not.
        var restore = ((IClosedShapeDocumentStorage<Doc, Guid>)storage).RestoreForId(id, "red")
            .ShouldBeOfType<RecordedOperation>();
        restore.Kind.ShouldBe("restore");
        restore.Fragment.ShouldBeNull();
    }

    [Theory]
    [InlineData(ClosedShapeTrackingMode.QueryOnly)]
    [InlineData(ClosedShapeTrackingMode.Lightweight)]
    [InlineData(ClosedShapeTrackingMode.IdentityMap)]
    [InlineData(ClosedShapeTrackingMode.DirtyTracking)]
    public void the_default_rules_have_nothing_to_undelete_or_restore(ClosedShapeTrackingMode mode)
    {
        var storage = ClosedShapeDocumentProviderFactory.BuildStorage(mode, descriptor(ConcurrencyMode.Off),
            new FakeRules<Doc>());

        storage.UndeleteFragment.ShouldBeNull();
        storage.UndeleteForId(Guid.NewGuid(), "t").ShouldBeNull();
        storage.RestoreForId(Guid.NewGuid(), "t").ShouldBeNull();
    }

    // ---- version mapping: the write half of a mapped member (Polecat's shape) ----

    [Fact]
    public void a_mapped_version_is_read_and_written_through_the_rules_object()
    {
        var storage = ClosedShapeDocumentProviderFactory.BuildStorage(ClosedShapeTrackingMode.IdentityMap,
            closed_shape_document_storage_family.descriptor<VersionedDoc>(ConcurrencyMode.Optimistic, d => d.Id), new MappedVersionRules());
        var doc = new VersionedDoc { Id = Guid.NewGuid() };
        var version = Guid.NewGuid();

        IDocumentStorage<VersionedDoc> asInterface = storage;
        asInterface.TryApplyMappedVersion(doc, version).ShouldBeTrue();
        doc.Version.ShouldBe(version);
        asInterface.MappedVersionFor(doc).ShouldBe(version);
    }

    [Fact]
    public void a_mapped_int_revision_narrows_on_the_way_in_and_widens_on_the_way_out()
    {
        var storage = ClosedShapeDocumentProviderFactory.BuildStorage(ClosedShapeTrackingMode.Lightweight,
            closed_shape_document_storage_family.descriptor<VersionedDoc>(ConcurrencyMode.Numeric, d => d.Id), new MappedVersionRules());
        var doc = new VersionedDoc { Id = Guid.NewGuid() };

        storage.TryApplyMappedRevision(doc, 7L).ShouldBeTrue();
        doc.Revision.ShouldBe(7);
        storage.MappedRevisionFor(doc).ShouldBe(7L);
    }

    [Fact]
    public void the_default_rules_map_no_version_member()
    {
        var storage = build(ClosedShapeTrackingMode.Lightweight, ConcurrencyMode.Optimistic);

        storage.TryApplyMappedVersion(new Doc(), Guid.NewGuid()).ShouldBeFalse();
        storage.TryApplyMappedRevision(new Doc(), 3).ShouldBeFalse();
        storage.MappedVersionFor(new Doc()).ShouldBeNull();
    }

    // ---- hierarchy sub-classes: the wrapper all three stores wrote ----

    [Fact]
    public void a_sub_class_storage_is_the_hierarchys_table_and_its_own_type()
    {
        var sub = subClassStorage(out var root);

        sub.SourceType.ShouldBe(typeof(Animal));
        sub.DocumentType.ShouldBe(typeof(Cat));
        sub.SelectedType.ShouldBe(typeof(Cat));
        sub.TableName.ShouldBe(root.TableName);
        sub.SelectFields().ShouldBe(root.SelectFields());
        sub.BuildSelector(theSession).ShouldBeOfType<StorageCastingSelector<Cat, Animal>>();
    }

    [Fact]
    public void a_sub_class_query_carries_the_type_filter_beside_the_roots_filters()
    {
        var subRules = new CatRules();
        var rootRules = new AnimalRules { DefaultWhere = new LiteralFragment("d.is_deleted = false") };
        var root = ClosedShapeDocumentProviderFactory.BuildStorage(ClosedShapeTrackingMode.Lightweight,
            closed_shape_document_storage_family.descriptor<Animal>(ConcurrencyMode.Off, a => a.Id), rootRules);
        var sub = ClosedShapeDocumentProviderFactory.BuildSubClassStorage(root, subRules);

        render(sub.DefaultWhereFragment()!).ShouldBe("(d.is_deleted = false) and (d.doc_type in ('cat', 'lion'))");
        render(sub.FilterDocuments(new LiteralFragment("d.name = 'x'"), theSession))
            .ShouldBe("(d.name = 'x') and (d.doc_type in ('cat', 'lion'))");

        // The compound keeps its children visible to a store searching the tree.
        sub.DefaultWhereFragment().ShouldBeAssignableTo<ICompoundFragment>()!.Children
            .ShouldContain(rootRules.DefaultWhere);
    }

    [Fact]
    public void with_no_root_default_filter_the_sub_class_default_is_the_type_filter_alone()
    {
        var subRules = new CatRules();
        var sub = subClassStorage(out _, subRules);

        sub.DefaultWhereFragment().ShouldBeSameAs(subRules.DocumentTypeFilter);
    }

    [Fact]
    public void a_store_whose_query_layer_adds_the_type_filter_itself_passes_the_query_through()
    {
        var sub = subClassStorage(out _, new PassThroughCatRules());
        var query = new LiteralFragment("d.name = 'x'");

        sub.FilterDocuments(query, theSession).ShouldBeSameAs(query);
    }

    [Fact]
    public async Task a_sub_class_load_narrows_to_the_sub_class_in_memory()
    {
        var sub = subClassStorage(out var root, mode: ClosedShapeTrackingMode.IdentityMap);
        var lion = new Lion { Id = Guid.NewGuid() };
        var dog = new Dog { Id = Guid.NewGuid() };
        root.Store(theSession, lion);
        root.Store(theSession, dog);

        (await sub.LoadAsync(lion.Id, theSession, CancellationToken.None)).ShouldBeSameAs(lion);
        (await sub.LoadAsync(dog.Id, theSession, CancellationToken.None)).ShouldBeNull();
        (await sub.LoadManyAsync([dog.Id, lion.Id], theSession, CancellationToken.None)).ShouldBe([lion]);
    }

    [Fact]
    public void a_sub_class_writes_through_the_root_untouched()
    {
        var rootRules = new AnimalRules();
        var root = ClosedShapeDocumentProviderFactory.BuildStorage(ClosedShapeTrackingMode.IdentityMap,
            closed_shape_document_storage_family.descriptor<Animal>(ConcurrencyMode.Optimistic, a => a.Id), rootRules);
        var sub = ClosedShapeDocumentProviderFactory.BuildSubClassStorage(root, new CatRules());
        var cat = new Cat { Id = Guid.NewGuid() };

        sub.Upsert(cat, theSession, "t").ShouldBeOfType<OptimisticClosedShapeUpsertOperation<Animal, Guid>>();
        sub.Store(theSession, cat);
        ((Dictionary<Guid, Animal>)theSession.ItemMap[typeof(Animal)])[cat.Id].ShouldBeSameAs(cat);
        rootRules.WriteRequests.ShouldBe([(OperationRole.Upsert, true)]);
    }

    [Fact]
    public void a_sub_class_forwards_the_mapped_version_members_to_its_root()
    {
        var root = ClosedShapeDocumentProviderFactory.BuildStorage(ClosedShapeTrackingMode.Lightweight,
            closed_shape_document_storage_family.descriptor<Animal>(ConcurrencyMode.Optimistic, a => a.Id), new AnimalRules());
        IDocumentStorage<Cat> sub = ClosedShapeDocumentProviderFactory.BuildSubClassStorage(root, new CatRules());
        var cat = new Cat { Id = Guid.NewGuid() };
        var version = Guid.NewGuid();

        sub.TryApplyMappedVersion(cat, version).ShouldBeTrue();
        sub.MappedVersionFor(cat).ShouldBe(version);
    }

    [Fact]
    public async Task a_sub_class_truncate_is_the_roots_unless_the_rules_narrow_it()
    {
        var database = Substitute.For<IStorageDatabase>();

        await subClassStorage(out _).TruncateDocumentStorageAsync(database, TestContext.Current.CancellationToken);
        await database.Received().RunSqlAsync("truncate animals", Arg.Any<CancellationToken>());

        await subClassStorage(out _, new CatRules { TruncateSql = "delete from animals where doc_type = 'cat'" })
            .TruncateDocumentStorageAsync(database, TestContext.Current.CancellationToken);
        await database.Received().RunSqlAsync("delete from animals where doc_type = 'cat'", Arg.Any<CancellationToken>());
    }

    [Fact]
    public void build_sub_class_wraps_each_of_the_roots_modes_in_the_same_mode()
    {
        var root = ClosedShapeDocumentProviderFactory.Build(closed_shape_document_storage_family.descriptor<Animal>(ConcurrencyMode.Off, a => a.Id),
            new AnimalRules());

        var provider = ClosedShapeDocumentProviderFactory.BuildSubClass(root, new CatRules());

        rootOf(provider.QueryOnly).ShouldBeSameAs(root.QueryOnly);
        rootOf(provider.Lightweight).ShouldBeSameAs(root.Lightweight);
        rootOf(provider.IdentityMap).ShouldBeSameAs(root.IdentityMap);
        rootOf(provider.DirtyTracking).ShouldBeSameAs(root.DirtyTracking);
    }

    [Fact]
    public void build_sub_class_refuses_a_root_keyed_by_another_id_type()
    {
        var root = ClosedShapeDocumentProviderFactory.Build(closed_shape_document_storage_family.descriptor<Animal>(ConcurrencyMode.Off, a => a.Id),
            new AnimalRules());

        Should.Throw<ArgumentException>(() =>
            ClosedShapeDocumentProviderFactory.BuildSubClass(root, new StringKeyedCatRules()));
    }

    [Fact]
    public void a_store_derives_the_sub_class_storage_to_carry_its_own_interface()
    {
        var root = ClosedShapeDocumentProviderFactory.BuildStorage(ClosedShapeTrackingMode.Lightweight,
            closed_shape_document_storage_family.descriptor<Animal>(ConcurrencyMode.Off, a => a.Id), new AnimalRules());

        IDocumentStorage<Cat> sub = new StoreSubClass(root, new CatRules());

        sub.ShouldBeAssignableTo<IStoreOwnStorage>();
        sub.SourceType.ShouldBe(typeof(Animal));
    }

    // ---- additive: every member added for weasel#811 to a public interface has a default body ----

    [Theory]
    [InlineData(typeof(IClosedShapeStorageRules<,>), nameof(IClosedShapeStorageRules<Doc, Guid>.TryApplyMappedVersion))]
    [InlineData(typeof(IClosedShapeStorageRules<,>), nameof(IClosedShapeStorageRules<Doc, Guid>.TryApplyMappedRevision))]
    [InlineData(typeof(IClosedShapeStorageRules<,>), nameof(IClosedShapeStorageRules<Doc, Guid>.BuildUndeleteFragment))]
    [InlineData(typeof(IClosedShapeStorageRules<,>), nameof(IClosedShapeStorageRules<Doc, Guid>.BuildUndeletion))]
    [InlineData(typeof(IClosedShapeStorageRules<,>), nameof(IClosedShapeStorageRules<Doc, Guid>.BuildRestoration))]
    [InlineData(typeof(IDocumentStorage<>), nameof(IDocumentStorage<Doc>.TryApplyMappedVersion))]
    [InlineData(typeof(IDocumentStorage<>), nameof(IDocumentStorage<Doc>.TryApplyMappedRevision))]
    [InlineData(typeof(IClosedShapeDocumentStorage), "get_" + nameof(IClosedShapeDocumentStorage.UndeleteFragment))]
    [InlineData(typeof(IClosedShapeDocumentStorage<,>), nameof(IClosedShapeDocumentStorage<Doc, Guid>.UndeleteForId))]
    [InlineData(typeof(IClosedShapeDocumentStorage<,>), nameof(IClosedShapeDocumentStorage<Doc, Guid>.RestoreForId))]
    public void a_member_added_to_a_public_interface_has_a_default_body(Type interfaceType, string method)
    {
        interfaceType.GetMethod(method, BindingFlags.Public | BindingFlags.Instance)
            .ShouldNotBeNull()
            .IsAbstract.ShouldBeFalse();
    }

    // ---- helpers ----

    private IClosedShapeDocumentStorage<Doc, Guid> build(ClosedShapeTrackingMode mode, ConcurrencyMode concurrency)
        => ClosedShapeDocumentProviderFactory.BuildStorage(mode, descriptor(concurrency), new FakeRules<Doc>());

    private static object rootOf(IDocumentStorage<Cat> storage)
        => storage.ShouldBeOfType<ClosedShapeSubClassDocumentStorage<Cat, Animal, Guid>>().Root;

    private static IDocumentStorage<Cat, Guid> subClassStorage(out IClosedShapeDocumentStorage<Animal, Guid> root,
        IClosedShapeSubClassRules<Cat, Animal, Guid>? rules = null,
        ClosedShapeTrackingMode mode = ClosedShapeTrackingMode.Lightweight)
    {
        root = ClosedShapeDocumentProviderFactory.BuildStorage(mode,
            closed_shape_document_storage_family.descriptor<Animal>(ConcurrencyMode.Off, a => a.Id), new AnimalRules());
        return ClosedShapeDocumentProviderFactory.BuildSubClassStorage(root, rules ?? new CatRules());
    }

    private static string[] writesOf(IDocumentStorage<Doc, Guid> storage, Doc doc)
        => new Weasel.Storage.IStorageOperation[]
        {
            storage.Insert(doc, new FakeSession(), "t"), storage.Update(doc, new FakeSession(), "t"),
            storage.Upsert(doc, new FakeSession(), "t"), storage.Overwrite(doc, new FakeSession(), "t"),
            storage.InsertProjected(doc, "t"), storage.UpdateProjected(doc, "t"), storage.UpsertProjected(doc, "t"),
            storage.OverwriteProjected(doc, "t")
        }.Select(x => x.GetType().Name).ToArray();

    private static string render(ISqlFragment fragment)
    {
        var sql = new System.Text.StringBuilder();
        var builder = Substitute.For<ICommandBuilder>();
        builder.When(x => x.Append(Arg.Any<string>())).Do(x => sql.Append(x.Arg<string>()));
        builder.When(x => x.Append(Arg.Any<char>())).Do(x => sql.Append(x.Arg<char>()));
        fragment.Apply(builder);
        return sql.ToString();
    }

    private static DocumentStorageDescriptor<Doc, Guid> descriptor(ConcurrencyMode concurrency,
        bool hierarchical = false)
        => hierarchical
            ? new(new GuidIdentification<Doc>(d => d.Id), Substitute.For<IStorageSerializer>(),
                Substitute.For<IStorageDialect>(), [], [], [], [], "upsert", "insert", "update", "overwrite", false,
                concurrency, null, null, -1, _ => typeof(Doc), 2, "docs")
            : closed_shape_document_storage_family.descriptor<Doc>(concurrency, d => d.Id);

    // ---- what a store writes ----

    private interface IStoreOwnStorage;

    private sealed class StoreQueryOnly(DocumentStorageDescriptor<Doc, Guid> descriptor, IClosedShapeStorageRules<Doc, Guid> rules)
        : QueryOnlyClosedShapeDocumentStorage<Doc, Guid>(descriptor, rules), IStoreOwnStorage;

    private sealed class StoreLightweight(DocumentStorageDescriptor<Doc, Guid> descriptor, IClosedShapeStorageRules<Doc, Guid> rules)
        : LightweightClosedShapeDocumentStorage<Doc, Guid>(descriptor, rules), IStoreOwnStorage;

    private sealed class StoreIdentityMap(DocumentStorageDescriptor<Doc, Guid> descriptor, IClosedShapeStorageRules<Doc, Guid> rules)
        : IdentityMapClosedShapeDocumentStorage<Doc, Guid>(descriptor, rules), IStoreOwnStorage;

    private sealed class StoreDirtyTracking(DocumentStorageDescriptor<Doc, Guid> descriptor, IClosedShapeStorageRules<Doc, Guid> rules)
        : DirtyTrackingClosedShapeDocumentStorage<Doc, Guid>(descriptor, rules), IStoreOwnStorage;

    private sealed class StoreSubClass(IDocumentStorage<Animal, Guid> root, IClosedShapeSubClassRules<Cat, Animal, Guid> rules)
        : ClosedShapeSubClassDocumentStorage<Cat, Animal, Guid>(root, rules), IStoreOwnStorage;

    /// <summary>Overrides every overridable member of a lightweight storage, recording each call.</summary>
    private sealed class HookedLightweight(DocumentStorageDescriptor<Doc, Guid> descriptor, IClosedShapeStorageRules<Doc, Guid> rules)
        : LightweightClosedShapeDocumentStorage<Doc, Guid>(descriptor, rules)
    {
        public List<string> Hits { get; } = [];
        public Doc Projected { get; } = new();

        private T hit<T>(string member, T result)
        {
            Hits.Add(member);
            return result;
        }

        public override Weasel.Storage.IStorageOperation Insert(Doc document, IStorageSession session, string tenantId)
            => hit(nameof(Insert), base.Insert(document, session, tenantId));

        public override Weasel.Storage.IStorageOperation Update(Doc document, IStorageSession session, string tenantId)
            => hit(nameof(Update), base.Update(document, session, tenantId));

        public override Weasel.Storage.IStorageOperation Upsert(Doc document, IStorageSession session, string tenantId)
            => hit(nameof(Upsert), base.Upsert(document, session, tenantId));

        public override Weasel.Storage.IStorageOperation Overwrite(Doc document, IStorageSession session, string tenantId)
            => hit(nameof(Overwrite), base.Overwrite(document, session, tenantId));

        public override Weasel.Storage.IStorageOperation InsertProjected(Doc document, string tenantId)
            => hit(nameof(InsertProjected), base.InsertProjected(document, tenantId));

        public override Weasel.Storage.IStorageOperation UpdateProjected(Doc document, string tenantId)
            => hit(nameof(UpdateProjected), base.UpdateProjected(document, tenantId));

        public override Weasel.Storage.IStorageOperation UpsertProjected(Doc document, string tenantId)
            => hit(nameof(UpsertProjected), base.UpsertProjected(document, tenantId));

        public override Weasel.Storage.IStorageOperation OverwriteProjected(Doc document, string tenantId)
            => hit(nameof(OverwriteProjected), base.OverwriteProjected(document, tenantId));

        public override ISelector BuildSelector(IStorageSession session)
            => hit(nameof(BuildSelector), base.BuildSelector(session));

        // A store's guard: Fisher short-circuits these rather than open a connection for nothing.
        public override Task<Doc?> LoadProjectedAsync(Guid id, IStorageDatabase database, string tenantId,
            CancellationToken token)
            => hit(nameof(LoadProjectedAsync), Task.FromResult<Doc?>(Projected));

        public override Task<IReadOnlyList<Doc>> LoadManyProjectedAsync(Guid[] ids, IStorageDatabase database,
            string tenantId, CancellationToken token)
            => ids.Length == 0
                ? hit(nameof(LoadManyProjectedAsync), Task.FromResult<IReadOnlyList<Doc>>([]))
                : base.LoadManyProjectedAsync(ids, database, tenantId, token);

        public override void ShareTenantNeutralStateWith(IStorageSession parent, IStorageSession nested)
            => hit(nameof(ShareTenantNeutralStateWith), 0);
    }

    private sealed record RecordedOperation(string Kind, IOperationFragment? Fragment, Guid Id, string TenantId)
        : Weasel.Storage.IStorageOperation
    {
        public Type DocumentType => typeof(Doc);
        public OperationRole Role() => OperationRole.Update;
        public void ConfigureCommand(ICommandBuilder builder, IStorageSession session) { }

        public Task PostprocessAsync(System.Data.Common.DbDataReader reader, IList<Exception> exceptions,
            CancellationToken token) => Task.CompletedTask;
    }

    /// <summary>Fisher's and Polecat's soft-delete shape: an undelete fragment, undelete and restore by id.</summary>
    private sealed class SoftDeletingRules: FakeRules<Doc>, IClosedShapeStorageRules<Doc, Guid>
    {
        public IOperationFragment UndeleteFragment { get; } = Substitute.For<IOperationFragment>();

        public IOperationFragment? BuildUndeleteFragment(IDocumentStorage<Doc, Guid> storage) => UndeleteFragment;

        public Weasel.Storage.IStorageOperation? BuildUndeletion(IDocumentStorage<Doc, Guid> storage, IOperationFragment? undeleteFragment,
            Guid id, ISqlFragment byIdFilter, string tenantId)
            => new RecordedOperation("undelete", undeleteFragment, id, tenantId);

        public Weasel.Storage.IStorageOperation? BuildRestoration(IDocumentStorage<Doc, Guid> storage, Guid id, ISqlFragment byIdFilter,
            string tenantId)
            => new RecordedOperation("restore", null, id, tenantId);
    }

    public class VersionedDoc
    {
        public Guid Id { get; set; }
        public Guid Version { get; set; }
        public int Revision { get; set; }
    }

    /// <summary>Polecat's mapped-concurrency shape: read and write a member mapped by configuration.</summary>
    private sealed class MappedVersionRules: FakeRules<VersionedDoc>, IClosedShapeStorageRules<VersionedDoc, Guid>
    {
        public Guid? MappedVersionFor(VersionedDoc document) => document.Version;
        public long? MappedRevisionFor(VersionedDoc document) => document.Revision;

        public bool TryApplyMappedVersion(VersionedDoc document, Guid version)
        {
            document.Version = version;
            return true;
        }

        public bool TryApplyMappedRevision(VersionedDoc document, long revision)
        {
            document.Revision = (int)revision;
            return true;
        }
    }

    public class Animal
    {
        public Guid Id { get; set; }
        public Guid Version { get; set; }
    }

    public class Cat: Animal;

    public class Lion: Cat;

    public class Dog: Animal;

    private sealed class AnimalRules: FakeRules<Animal>, IClosedShapeStorageRules<Animal, Guid>
    {
        public ISqlFragment? DefaultWhere { get; init; }
        public new string TruncateSql => "truncate animals";
        public new ISqlFragment? DefaultWhereFragment() => DefaultWhere;
        public Guid? MappedVersionFor(Animal document) => document.Version;

        public bool TryApplyMappedVersion(Animal document, Guid version)
        {
            document.Version = version;
            return true;
        }
    }

    /// <summary>Marten's and Polecat's shape: the type filter on every query, truncate optional.</summary>
    private class CatRules: IClosedShapeSubClassRules<Cat, Animal, Guid>
    {
        public ISqlFragment DocumentTypeFilter { get; } = new LiteralFragment("d.doc_type in ('cat', 'lion')");
        public string? TruncateSql { get; init; }
    }

    /// <summary>Fisher's shape: its query layer adds the type filter once per statement.</summary>
    private sealed class PassThroughCatRules: CatRules, IClosedShapeSubClassRules<Cat, Animal, Guid>
    {
        public ISqlFragment FilterDocuments(IDocumentStorage<Animal, Guid> root, ISqlFragment query,
            IStorageSession session)
            => root.FilterDocuments(query, session);
    }

    private sealed class StringKeyedCatRules: IClosedShapeSubClassRules<Cat, Animal, string>
    {
        public ISqlFragment DocumentTypeFilter { get; } = new LiteralFragment("d.doc_type = 'cat'");
    }

    private sealed class LiteralFragment(string sql): ISqlFragment
    {
        public void Apply(ICommandBuilder builder) => builder.Append(sql);
    }
}
