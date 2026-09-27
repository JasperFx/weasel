using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

public class JsonColumnDbContext : DbContext
{
    public static readonly string ConnectionString = PostgresqlDbContext.ConnectionString;

    public JsonColumnDbContext(DbContextOptions<JsonColumnDbContext> options) : base(options)
    {
    }

    public DbSet<ZEntityWithJsonColumn> ZEntities => Set<ZEntityWithJsonColumn>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ZEntityWithJsonColumn>(entity =>
        {
            entity.ToTable("zentities", "ef_json_test");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.InternalName).HasColumnName("internal_name");
            entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(100);
            entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(2000);
            entity.OwnsOne(e => e.AExtendedProperties, b =>
            {
                b.ToJson("aextended_properties");
            });
        });
    }
}
