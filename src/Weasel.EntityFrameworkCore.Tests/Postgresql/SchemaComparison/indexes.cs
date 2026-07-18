using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     Index permutations: single-column, composite (column order), unique,
///     custom-named (HasDatabaseName), filtered/partial (HasFilter), covering
///     (IncludeProperties), plus EF's conventional FK index. GIN method and
///     descending indexes are covered separately via the customizeTables
///     escape hatch since ITableIndex intentionally models only the
///     cross-provider common denominator.
/// </summary>
public class indexes
{
    public const string SchemaName = "efcmp_indexes";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new IndexesDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();

        var table = result.EfSchema.TableFor("Articles")!;
        table.IndexFor("IX_Articles_Slug")!.IsUnique.ShouldBeTrue();
        table.IndexFor("IX_Articles_Category_PublishedOn")!.KeyColumns.ShouldBe(["Category", "PublishedOn"]);
        table.IndexFor("ix_articles_active")!.Predicate.ShouldNotBeNull();
        table.IndexFor("IX_Articles_Category_PublishedOn")!.IsUnique.ShouldBeFalse();
        table.IndexFor("IX_Articles_Category_Covering")!.IncludedColumns.ShouldBe(["Title"]);

        // the conventional FK index must exist on BOTH sides
        result.EfSchema.TableFor("ArticleComments")!.IndexFor("IX_ArticleComments_ArticleId").ShouldNotBeNull();
        result.WeaselSchema.TableFor("ArticleComments")!.IndexFor("IX_ArticleComments_ArticleId").ShouldNotBeNull();
    }

    /// <summary>
    ///     Provider-specific index features (Npgsql HasMethod, descending sort)
    ///     are not expressible through the provider-neutral ITableIndex seam.
    ///     The harness's customizeTables hook is the supported escape hatch:
    ///     downcast to the concrete provider table and enrich the definition.
    /// </summary>
    [Fact]
    public async Task gin_and_descending_indexes_via_customize_escape_hatch()
    {
        await using var context = new ProviderIndexesDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, ProviderIndexesDbContext.SchemaName,
            tables =>
            {
                var docs = tables.OfType<Table>().Single(t => t.Identifier.Name == "TaggedDocs");
                docs.IndexFor("IX_TaggedDocs_Tags")!.Method = IndexMethod.gin;
                docs.IndexFor("IX_TaggedDocs_Rank")!.SortOrder = SortOrder.Desc;
            });

        result.AssertParity();

        result.WeaselSchema.TableFor("TaggedDocs")!.IndexFor("IX_TaggedDocs_Tags")!.Method.ShouldBe("gin");
        result.WeaselSchema.TableFor("TaggedDocs")!.IndexFor("IX_TaggedDocs_Rank")!.IsDescending.ShouldBe([true]);
    }
}

public class ArticleEntity
{
    public int Id { get; set; }
    public string Slug { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public DateOnly PublishedOn { get; set; }
    public bool Active { get; set; }
}

public class ArticleComment
{
    public int Id { get; set; }
    public int ArticleId { get; set; }
    public string Body { get; set; } = string.Empty;
    public ArticleEntity Article { get; set; } = null!;
}

public class IndexesDbContext : DbContext
{
    public DbSet<ArticleEntity> Articles => Set<ArticleEntity>();
    public DbSet<ArticleComment> ArticleComments => Set<ArticleComment>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(indexes.SchemaName);

        modelBuilder.Entity<ArticleEntity>(entity =>
        {
            entity.ToTable("Articles");
            entity.HasIndex(e => e.Slug).IsUnique();
            entity.HasIndex(e => new { e.Category, e.PublishedOn });
            entity.HasIndex(e => e.PublishedOn)
                .HasDatabaseName("ix_articles_active")
                .HasFilter("\"Active\"");
            // Named overload: a second unnamed HasIndex over the same properties
            // would silently refer to (and rename) the unique Slug index above
            NpgsqlIndexBuilderExtensions.IncludeProperties(
                entity.HasIndex(["Category"], "IX_Articles_Category_Covering"),
                e => e.Title);
        });

        modelBuilder.Entity<ArticleComment>(entity =>
        {
            entity.ToTable("ArticleComments");
            entity.HasOne(e => e.Article)
                .WithMany()
                .HasForeignKey(e => e.ArticleId)
                .OnDelete(DeleteBehavior.Cascade);
        });
    }
}

public class TaggedDoc
{
    public int Id { get; set; }
    public string[] Tags { get; set; } = [];
    public int Rank { get; set; }
}

public class ProviderIndexesDbContext : DbContext
{
    public const string SchemaName = "efcmp_pgindexes";

    public DbSet<TaggedDoc> TaggedDocs => Set<TaggedDoc>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(SchemaName);

        modelBuilder.Entity<TaggedDoc>(entity =>
        {
            entity.ToTable("TaggedDocs");
            entity.HasIndex(e => e.Tags).HasMethod("gin");
            entity.HasIndex(e => e.Rank).IsDescending();
        });
    }
}
