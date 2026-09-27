using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

/// <summary>
/// DbContext with FK dependencies between entities and an explicit schema.
/// Used to test that GetEntityTypesForMigration returns entities in dependency
/// order (referenced tables before referencing tables).
/// See https://github.com/JasperFx/marten/issues/4180
/// </summary>
public class FkDependencyDbContext : DbContext
{
    public static readonly string ConnectionString = PostgresqlDbContext.ConnectionString;
    public const string TestSchema = "fk_dep_test";

    public FkDependencyDbContext(DbContextOptions<FkDependencyDbContext> options) : base(options)
    {
    }

    public DbSet<DependentEntity> DependentEntities => Set<DependentEntity>();
    public DbSet<EntityCategory> EntityCategories => Set<EntityCategory>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(TestSchema);

        modelBuilder.Entity<EntityCategory>(entity =>
        {
            entity.ToTable("entity_category");
            entity.HasKey(e => e.Id);
        });

        modelBuilder.Entity<DependentEntity>(entity =>
        {
            entity.ToTable("dependent_entity");
            entity.HasKey(e => e.Id);
            entity.HasOne(e => e.Category)
                .WithMany()
                .HasForeignKey(e => e.CategoryId)
                .OnDelete(DeleteBehavior.Cascade);
        });
    }
}
