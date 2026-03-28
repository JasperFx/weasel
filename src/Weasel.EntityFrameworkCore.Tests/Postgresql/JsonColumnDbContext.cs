using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

public class JsonColumnDbContext : DbContext
{
    public const string ConnectionString = "Host=localhost;Port=5432;Database=marten_testing;Username=postgres;Password=postgres";

    public JsonColumnDbContext(DbContextOptions<JsonColumnDbContext> options) : base(options)
    {
    }

    public DbSet<EntityWithJsonColumn> Entities => Set<EntityWithJsonColumn>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<EntityWithJsonColumn>(entity =>
        {
            entity.ToTable("entities", "ef_json_test");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.InternalName).HasColumnName("internal_name");
            entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(100);
            entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(2000);
            entity.OwnsOne(e => e.ExtendedProperties, b =>
            {
                b.ToJson("extended_properties");
            });
        });
    }
}
