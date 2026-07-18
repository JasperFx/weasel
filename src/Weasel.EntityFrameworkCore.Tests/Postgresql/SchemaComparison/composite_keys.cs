using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     Composite key permutations: a two-column composite PK whose constraint
///     order comes from the HasKey lambda (TenantId, Id — not model declaration
///     order), a composite FK referencing it, and HasColumnOrder reordering the
///     physical columns independently of the PK order.
/// </summary>
public class composite_keys
{
    public const string SchemaName = "efcmp_compkeys";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new CompositeKeyDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();

        // PK column ORDER must match the HasKey lambda on both sides
        result.EfSchema.TableFor("Tenants")!.PrimaryKeyColumns.ShouldBe(["TenantId", "Id"]);
        result.WeaselSchema.TableFor("Tenants")!.PrimaryKeyColumns.ShouldBe(["TenantId", "Id"]);

        // Composite FK column order must align with the principal key order
        var fk = result.WeaselSchema.TableFor("TenantDocuments")!.ForeignKeys.Single();
        fk.Columns.ShouldBe(["TenantId", "OwnerId"]);
        fk.PrincipalColumns.ShouldBe(["TenantId", "Id"]);
    }
}

public class TenantEntity
{
    public int Id { get; set; }
    public int TenantId { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class TenantDocument
{
    public Guid Id { get; set; }
    public int TenantId { get; set; }
    public int OwnerId { get; set; }
    public string Body { get; set; } = string.Empty;
    public TenantEntity Owner { get; set; } = null!;
}

public class CompositeKeyDbContext : DbContext
{
    public DbSet<TenantEntity> Tenants => Set<TenantEntity>();
    public DbSet<TenantDocument> TenantDocuments => Set<TenantDocument>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(composite_keys.SchemaName);

        modelBuilder.Entity<TenantEntity>(entity =>
        {
            entity.ToTable("Tenants");
            // Lambda order (TenantId, Id) is the constraint order, distinct from
            // both property declaration order and physical column order
            entity.HasKey(e => new { e.TenantId, e.Id });
            entity.Property(e => e.Name).HasColumnOrder(1);
            entity.Property(e => e.TenantId).HasColumnOrder(2);
            entity.Property(e => e.Id).HasColumnOrder(3);
        });

        modelBuilder.Entity<TenantDocument>(entity =>
        {
            entity.ToTable("TenantDocuments");
            entity.HasKey(e => e.Id);
            entity.HasOne(e => e.Owner)
                .WithMany()
                .HasForeignKey(e => new { e.TenantId, e.OwnerId })
                .OnDelete(DeleteBehavior.Cascade);
        });
    }
}
