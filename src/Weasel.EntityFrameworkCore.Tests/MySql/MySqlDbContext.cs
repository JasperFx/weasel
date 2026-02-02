using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.MySql;

public class MySqlDbContext : DbContext
{
    public const string ConnectionString = "Server=localhost;Port=3306;Database=weasel_testing;User=weasel;Password=P@55w0rd";

    public MySqlDbContext(DbContextOptions<MySqlDbContext> options) : base(options)
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
