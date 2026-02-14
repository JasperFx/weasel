using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

public class PostgresqlDbContext : DbContext
{
    public const string ConnectionString = "Host=localhost;Port=5432;Database=marten_testing;Username=postgres;Password=postgres";

    public PostgresqlDbContext(DbContextOptions<PostgresqlDbContext> options) : base(options)
    {
    }

    public DbSet<MyEntity> MyEntities => Set<MyEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MyEntity>(entity =>
        {
            entity.ToTable("my_entities");
            entity.HasKey(e => e.Id);
        });
    }
}
