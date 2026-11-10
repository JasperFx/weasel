using NSubstitute;
using Shouldly;
using Weasel.Core.Identity;
using Weasel.Storage;
using Weasel.Storage.Metadata;
using Xunit;

namespace Weasel.Core.Tests.StorageHelpers;

/// <summary>
///     weasel#745: <see cref="DocumentBinderPlan{TDoc}" />, <see cref="DocumentMetadataColumns" /> and
///     <see cref="MetadataConfig{T}" />. The invariant the plan exists for is that binder order sets
///     the SELECT ordinals: the column list a store emits and the ordinal each binder reads at come
///     from the same array. Each store's real order is reproduced here (Marten, Polecat and Fisher do
///     not agree on it) so that a change to the plan which reorders anything fails here first.
/// </summary>
public class document_binder_plan
{
    private static readonly string[] Leading = ["id", "data"];
    private readonly IStorageDialect theDialect = Substitute.For<IStorageDialect>();

    // ---- each store's order, end to end ----

    [Fact]
    public void marten_order_hierarchy_version_tenant_then_the_rest()
    {
        var columns = martenColumns();
        columns.Version.Member = member(nameof(PlanDoc.Version));
        columns.TenantId.Member = member(nameof(PlanDoc.TenantId));
        columns.LastModified.Member = member(nameof(PlanDoc.LastModified));
        columns.CreatedAt.Member = member(nameof(PlanDoc.CreatedAt));
        columns.CorrelationId.Enabled = true;
        columns.CorrelationId.Member = member(nameof(PlanDoc.CorrelationId));
        columns.Headers.Enabled = true;

        var plan = new DocumentBinderPlan<PlanDoc>()
            .DocumentType(new DocumentDocTypeBinder<PlanDoc>(columns.DocumentType.Name, theDialect, t => t.Name))
            .Version(new DocumentVersionBinder<PlanDoc>(columns.Version.Name, theDialect, columns.Version.Member),
                read: true, readByQueryOnly: columns.Version.Member is not null)
            .Write(new DocumentDotNetTypeBinder<PlanDoc>(columns.DotNetType.Name, theDialect))
            .ReadIfMapped(new DocumentTenantIdBinder<PlanDoc>(columns.TenantId.Name, columns.TenantId.Member),
                columns.TenantId)
            .AddIfEnabled(columns.LastModified, (n, m) => new DocumentLastModifiedBinder<PlanDoc>(n, m))
            .ReadIfMapped(new DocumentCreatedAtBinder<PlanDoc>(columns.CreatedAt.Name, columns.CreatedAt.Member),
                columns.CreatedAt)
            .AddIfEnabled(columns.CorrelationId, (n, m) => new DocumentCorrelationIdBinder<PlanDoc>(n, theDialect, m))
            .AddIfEnabled(columns.CausationId, (n, m) => new DocumentCausationIdBinder<PlanDoc>(n, theDialect, m))
            .Write(new DocumentHeadersBinder<PlanDoc>(columns.Headers.Name, theDialect, columns.Headers.Member));

        plan.SelectColumns(Leading).ShouldBe([
            "id", "data", "mt_doc_type", "mt_version", "tenant_id", "mt_last_modified", "mt_created_at",
            "correlation_id"
        ]);

        plan.WriteBinders.Select(x => x.ColumnName).ShouldBe([
            "mt_doc_type", "mt_version", "mt_dotnet_type", "mt_last_modified", "correlation_id", "headers"
        ]);

        // mt_last_modified is server-side by default, so it has no ? slot.
        plan.ClientSideWriteBinders.Select(x => x.ColumnName).ShouldBe([
            "mt_doc_type", "mt_version", "mt_dotnet_type", "correlation_id", "headers"
        ]);

        plan.DocTypeReadIndex.ShouldBe(0);
        plan.VersionReadIndex.ShouldBe(1);
        assertOrdinalsMatchTheSelect(plan);
    }

    [Fact]
    public void polecat_order_tenant_before_the_version()
    {
        var columns = new DocumentMetadataColumns();
        columns.Version.Enabled = true;
        columns.TenantId.Enabled = true;
        columns.TenantId.Member = member(nameof(PlanDoc.TenantId));
        columns.LastModified.Enabled = true;
        columns.CreatedAt.Enabled = true;
        columns.CreatedAt.Member = member(nameof(PlanDoc.CreatedAt));
        columns.DotNetType.Enabled = true;

        var plan = new DocumentBinderPlan<PlanDoc>()
            .ReadIfMapped(new DocumentTenantIdBinder<PlanDoc>(columns.TenantId.Name, columns.TenantId.Member),
                columns.TenantId)
            // Polecat: optimistic concurrency, document not IVersioned, so QueryOnly drops it.
            .Version(new DocumentVersionBinder<PlanDoc>(columns.Version.Name, theDialect, null),
                read: true, readByQueryOnly: false)
            .Write(new DocumentDotNetTypeBinder<PlanDoc>(columns.DotNetType.Name, theDialect))
            .AddIfEnabled(columns.LastModified,
                (n, m) => new DocumentLastModifiedBinder<PlanDoc>(n, m, "SYSDATETIMEOFFSET()"))
            .ReadIfMapped(new DocumentCreatedAtBinder<PlanDoc>(columns.CreatedAt.Name, columns.CreatedAt.Member,
                "SYSDATETIMEOFFSET()"), columns.CreatedAt);

        plan.SelectColumns(Leading).ShouldBe(["id", "data", "tenant_id", "guid_version", "created_at"]);
        plan.QueryOnlySelectColumns("data").ShouldBe(["data", "tenant_id", "created_at"]);
        plan.VersionReadIndex.ShouldBe(1);
        plan.DocTypeReadIndex.ShouldBe(-1);
        assertOrdinalsMatchTheSelect(plan);
    }

    [Fact]
    public void fisher_order_session_columns_then_created_at_then_tenant_last()
    {
        var columns = new DocumentMetadataColumns();
        columns.Revision.Enabled = true;
        columns.LastModified.Enabled = true;
        columns.LastModified.Member = member(nameof(PlanDoc.LastModified));
        columns.IsSoftDeleted.Enabled = true;
        columns.IsSoftDeleted.Member = member(nameof(PlanDoc.Deleted));
        columns.SoftDeletedAt.Enabled = true;
        columns.CorrelationId.Enabled = true;
        columns.CorrelationId.Member = member(nameof(PlanDoc.CorrelationId));
        columns.CreatedAt.Enabled = true;
        columns.CreatedAt.Member = member(nameof(PlanDoc.CreatedAt));
        columns.TenantId.Enabled = true;
        columns.TenantId.Member = member(nameof(PlanDoc.TenantId));

        var revision = new DocumentRevisionBinder<PlanDoc>(columns.Revision.Name, theDialect, null,
            StorageColumnType.Int);

        var plan = new DocumentBinderPlan<PlanDoc>()
            // Fisher keeps an unmapped revision in the QueryOnly set.
            .Revision(revision, read: true, readByQueryOnly: true)
            .Write(new DocumentDotNetTypeBinder<PlanDoc>("dotnet_type", theDialect))
            .AddIfEnabled(columns.LastModified,
                (n, m) => new DocumentLastModifiedBinder<PlanDoc>(n, m, "strftime('%Y-%m-%dT%H:%M:%fZ','now')"))
            .AddIfEnabled(columns.IsSoftDeleted, (n, m) => new DocumentSoftDeletedBinder<PlanDoc>(n, theDialect, m))
            .AddIfEnabled(columns.SoftDeletedAt,
                (n, m) => new DocumentSoftDeletedAtBinder<PlanDoc>(n, theDialect, m))
            .AddIfEnabled(columns.CorrelationId,
                (n, m) => new DocumentCorrelationIdBinder<PlanDoc>(n, theDialect, m))
            .ReadIfMapped(new DocumentCreatedAtBinder<PlanDoc>(columns.CreatedAt.Name, columns.CreatedAt.Member),
                columns.CreatedAt)
            .ReadIfMapped(new DocumentTenantIdBinder<PlanDoc>(columns.TenantId.Name, columns.TenantId.Member),
                columns.TenantId);

        plan.SelectColumns(Leading).ShouldBe([
            "id", "data", "revision", "last_modified", "is_deleted", "correlation_id", "created_at", "tenant_id"
        ]);

        // deleted_at is written but, unmapped, never read.
        plan.WriteBinders.Select(x => x.ColumnName).ShouldBe([
            "revision", "dotnet_type", "last_modified", "is_deleted", "deleted_at", "correlation_id"
        ]);

        plan.RevisionBinder.ShouldBeSameAs(revision);
        plan.VersionBinder.ShouldBeNull();
        plan.VersionReadIndex.ShouldBe(0);
        plan.QueryOnlyReadBinders.ShouldBeSameAs(plan.ReadBinders);
        assertOrdinalsMatchTheSelect(plan);
    }

    // ---- QueryOnly ----

    [Fact]
    public void query_only_drops_an_unread_version_and_shifts_what_follows()
    {
        var version = new DocumentVersionBinder<PlanDoc>("mt_version", theDialect, null);
        var lastModified = new DocumentLastModifiedBinder<PlanDoc>("mt_last_modified",
            member(nameof(PlanDoc.LastModified)));

        var plan = new DocumentBinderPlan<PlanDoc>()
            .Version(version, read: true, readByQueryOnly: false)
            .WriteAndRead(lastModified);

        plan.ReadBinders.ShouldBe([version, lastModified]);
        plan.QueryOnlyReadBinders.ShouldBe([lastModified]);

        plan.OrdinalOf(lastModified, 2).ShouldBe(3);
        plan.OrdinalOf(lastModified, 1, queryOnly: true).ShouldBe(1);
        plan.OrdinalOf(version, 1, queryOnly: true).ShouldBe(-1);
    }

    [Fact]
    public void query_only_is_the_same_array_when_nothing_is_dropped()
    {
        var plan = new DocumentBinderPlan<PlanDoc>()
            .Version(new DocumentVersionBinder<PlanDoc>("v", theDialect, member(nameof(PlanDoc.Version))),
                read: true, readByQueryOnly: true);

        plan.QueryOnlyReadBinders.ShouldBeSameAs(plan.ReadBinders);
    }

    [Fact]
    public void a_version_that_is_not_read_has_no_read_index_but_is_still_written()
    {
        var version = new DocumentVersionBinder<PlanDoc>("v", theDialect, null);
        var plan = new DocumentBinderPlan<PlanDoc>().Version(version, read: false, readByQueryOnly: false);

        plan.VersionReadIndex.ShouldBe(-1);
        plan.ReadBinders.ShouldBeEmpty();
        plan.WriteBinders.ShouldBe([version]);
        plan.VersionBinder.ShouldBeSameAs(version);
    }

    [Fact]
    public void refuses_a_doc_type_after_a_version_query_only_drops()
    {
        // The hierarchical QueryOnly selector reads the doc type at DocTypeReadIndex against the
        // QueryOnly set, so dropping a version ahead of it would read the wrong column.
        var plan = new DocumentBinderPlan<PlanDoc>()
            .Version(new DocumentVersionBinder<PlanDoc>("v", theDialect, null), read: true, readByQueryOnly: false)
            .DocumentType(new DocumentDocTypeBinder<PlanDoc>("doc_type", theDialect, t => t.Name));

        Should.Throw<InvalidOperationException>(() => plan.ReadBinders)
            .Message.ShouldContain("Add the document type first");
    }

    [Fact]
    public void plan_is_rebuilt_after_a_later_add()
    {
        var plan = new DocumentBinderPlan<PlanDoc>()
            .Write(new DocumentDotNetTypeBinder<PlanDoc>("dotnet_type", theDialect));
        plan.WriteBinders.Length.ShouldBe(1);

        plan.Write(new DocumentHeadersBinder<PlanDoc>("headers", theDialect, null));
        plan.WriteBinders.Length.ShouldBe(2);
    }

    // ---- guards ----

    [Fact]
    public void refuses_the_same_binder_twice_on_one_side()
    {
        var binder = new DocumentDotNetTypeBinder<PlanDoc>("dotnet_type", theDialect);
        var plan = new DocumentBinderPlan<PlanDoc>().Write(binder);

        Should.Throw<InvalidOperationException>(() => plan.Write(binder));
    }

    [Fact]
    public void refuses_two_binders_for_one_read_column()
    {
        var plan = new DocumentBinderPlan<PlanDoc>()
            .Read(new DocumentTenantIdBinder<PlanDoc>("tenant_id", member(nameof(PlanDoc.TenantId))));

        Should.Throw<InvalidOperationException>(() =>
                plan.Read(new DocumentTenantIdBinder<PlanDoc>("TENANT_ID", member(nameof(PlanDoc.TenantId)))))
            .Message.ShouldContain("tenant_id");
    }

    [Fact]
    public void refuses_a_version_and_a_revision()
    {
        var plan = new DocumentBinderPlan<PlanDoc>()
            .Version(new DocumentVersionBinder<PlanDoc>("guid_version", theDialect, null), true, true);

        Should.Throw<InvalidOperationException>(() => plan.Revision(
            new DocumentRevisionBinder<PlanDoc>("revision", theDialect, null), true, true));
    }

    [Fact]
    public void refuses_a_second_document_type()
    {
        var plan = new DocumentBinderPlan<PlanDoc>()
            .DocumentType(new DocumentDocTypeBinder<PlanDoc>("doc_type", theDialect, t => t.Name));

        Should.Throw<InvalidOperationException>(() =>
            plan.DocumentType(new DocumentDocTypeBinder<PlanDoc>("other", theDialect, t => t.Name)));
    }

    [Fact]
    public void a_disabled_column_adds_nothing()
    {
        var columns = new DocumentMetadataColumns();
        var created = false;

        var plan = new DocumentBinderPlan<PlanDoc>()
            .AddIfEnabled(columns.CorrelationId, (n, m) =>
            {
                created = true;
                return new DocumentCorrelationIdBinder<PlanDoc>(n, theDialect, m);
            });

        created.ShouldBeFalse();
        plan.WriteBinders.ShouldBeEmpty();
    }

    // ---- the descriptor overload ----

    [Fact]
    public void descriptor_built_from_a_plan_carries_the_plans_arrays()
    {
        var version = new DocumentVersionBinder<PlanDoc>("guid_version", theDialect, null);
        var docType = new DocumentDocTypeBinder<PlanDoc>("doc_type", theDialect, t => t.Name);
        var plan = new DocumentBinderPlan<PlanDoc>()
            .DocumentType(docType)
            .Version(version, read: true, readByQueryOnly: false)
            .Write(new DocumentDotNetTypeBinder<PlanDoc>("dotnet_type", theDialect));

        var descriptor = new DocumentStorageDescriptor<PlanDoc, Guid>(
            Substitute.For<IIdentification<PlanDoc, Guid>>(),
            Substitute.For<IStorageSerializer>(),
            theDialect,
            plan,
            "upsert", "insert", "update", "overwrite",
            isConjoined: false,
            ConcurrencyMode.Optimistic,
            resolveDocumentType: null,
            tableName: "plan_doc");

        descriptor.WriteBinders.ShouldBeSameAs(plan.WriteBinders);
        descriptor.ClientSideWriteBinders.ShouldBeSameAs(plan.ClientSideWriteBinders);
        descriptor.ReadBinders.ShouldBeSameAs(plan.ReadBinders);
        descriptor.QueryOnlyReadBinders.ShouldBeSameAs(plan.QueryOnlyReadBinders);
        descriptor.QueryOnlyReadBinders.ShouldBe([docType]);
        descriptor.VersionBinder.ShouldBeSameAs(version);
        descriptor.RevisionBinder.ShouldBeNull();
        descriptor.VersionReadIndex.ShouldBe(1);
        descriptor.DocTypeReadIndex.ShouldBe(0);
    }

    // ---- DocumentMetadataColumns ----

    [Fact]
    public void every_column_starts_disabled_and_unmapped()
    {
        var columns = new DocumentMetadataColumns();

        columns.AllColumns().Count().ShouldBe(13);
        columns.AllColumns().ShouldAllBe(x => !x.Enabled && x.Member == null);
        columns.AllColumns().Select(x => x.Name).Distinct().Count().ShouldBe(13);
        columns.EnabledColumns().ShouldBeEmpty();
        columns.SessionColumns().Select(x => x.Name)
            .ShouldBe(["correlation_id", "causation_id", "last_modified_by", "headers"]);
    }

    [Fact]
    public void merge_enables_and_maps_but_never_disables_or_renames()
    {
        var mine = new DocumentMetadataColumns();
        mine.LastModified.Enabled = true;
        mine.Version.Name = "mt_version";

        var theirs = new DocumentMetadataColumns();
        theirs.Version.Enabled = true;
        theirs.Version.Member = member(nameof(PlanDoc.Version));
        theirs.Version.Name = "other";

        mine.MergeFrom(theirs);

        mine.LastModified.Enabled.ShouldBeTrue();
        mine.Version.Enabled.ShouldBeTrue();
        mine.Version.Member.ShouldBe(member(nameof(PlanDoc.Version)));
        mine.Version.Name.ShouldBe("mt_version");
    }

    [Fact]
    public void refuses_a_version_and_a_revision_together()
    {
        var columns = new DocumentMetadataColumns();
        columns.Version.Enabled = true;
        columns.AssertValid(typeof(PlanDoc));

        columns.Revision.Enabled = true;
        Should.Throw<InvalidOperationException>(() => columns.AssertValid(typeof(PlanDoc)));
    }

    [Fact]
    public void a_column_needs_a_name()
    {
        Should.Throw<ArgumentException>(() => new DocumentMetadataColumn(" "));
        Should.Throw<ArgumentException>(() => new DocumentMetadataColumns().Headers.Name = "");
    }

    // ---- MetadataConfig<T> ----

    [Fact]
    public void map_to_sets_the_member_and_enables_the_column()
    {
        var columns = new DocumentMetadataColumns();
        var config = new MetadataConfig<PlanDoc>(columns);

        config.Version.MapTo(x => x.Version);
        config.CorrelationId.MapTo(x => x.CorrelationId);
        config.Headers.Enabled = true;

        columns.Version.EnabledWithMember.ShouldBeTrue();
        columns.Version.Member!.Name.ShouldBe(nameof(PlanDoc.Version));
        columns.CorrelationId.Member!.Name.ShouldBe(nameof(PlanDoc.CorrelationId));
        columns.Headers.Enabled.ShouldBeTrue();
        columns.Headers.Member.ShouldBeNull();
    }

    [Fact]
    public void every_dsl_property_writes_into_its_own_column()
    {
        var columns = new DocumentMetadataColumns();
        var config = new MetadataConfig<PlanDoc>(columns);

        MetadataColumnExpression<PlanDoc>[] expressions =
        [
            config.Version, config.Revision, config.LastModified, config.CreatedAt, config.TenantId,
            config.IsSoftDeleted, config.SoftDeletedAt, config.DocumentType, config.DotNetType,
            config.CorrelationId, config.CausationId, config.LastModifiedBy, config.Headers
        ];

        expressions.Select(x => x.Column).ShouldBe(columns.AllColumns().ToArray());
    }

    [Fact]
    public void map_to_refuses_anything_but_a_direct_member()
    {
        var config = new MetadataConfig<PlanDoc>(new DocumentMetadataColumns());

        Should.Throw<ArgumentException>(() => config.CorrelationId.MapTo(x => x.Name!.Length));
        Should.Throw<ArgumentException>(() => config.CorrelationId.MapTo(x => x.ToString()));
    }

    // ---- helpers ----

    private static void assertOrdinalsMatchTheSelect(DocumentBinderPlan<PlanDoc> plan)
    {
        var select = plan.SelectColumns(Leading);
        for (var i = 0; i < plan.ReadBinders.Length; i++)
        {
            var binder = plan.ReadBinders[i];
            select[plan.OrdinalOf(binder, Leading.Length)].ShouldBe(binder.ColumnName);
        }

        var queryOnly = plan.QueryOnlySelectColumns("data");
        foreach (var binder in plan.QueryOnlyReadBinders)
        {
            queryOnly[plan.OrdinalOf(binder, 1, queryOnly: true)].ShouldBe(binder.ColumnName);
        }

        if (plan.DocTypeReadIndex >= 0)
        {
            plan.QueryOnlyReadBinders[plan.DocTypeReadIndex].ShouldBeSameAs(plan.ReadBinders[plan.DocTypeReadIndex]);
        }
    }

    private static DocumentMetadataColumns martenColumns()
    {
        var columns = new DocumentMetadataColumns();
        columns.Version.Name = "mt_version";
        columns.Revision.Name = "mt_version";
        columns.LastModified.Name = "mt_last_modified";
        columns.CreatedAt.Name = "mt_created_at";
        columns.IsSoftDeleted.Name = "mt_deleted";
        columns.SoftDeletedAt.Name = "mt_deleted_at";
        columns.DocumentType.Name = "mt_doc_type";
        columns.DotNetType.Name = "mt_dotnet_type";

        columns.Version.Enabled = true;
        columns.LastModified.Enabled = true;
        columns.CreatedAt.Enabled = true;
        columns.DotNetType.Enabled = true;
        columns.TenantId.Enabled = true;
        return columns;
    }

    private static System.Reflection.MemberInfo member(string name) => typeof(PlanDoc).GetProperty(name)!;

    public class PlanDoc
    {
        public Guid Id { get; set; }
        public Guid Version { get; set; }
        public DateTimeOffset LastModified { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public string? TenantId { get; set; }
        public string? CorrelationId { get; set; }
        public bool Deleted { get; set; }
        public string? Name { get; set; }
    }
}
