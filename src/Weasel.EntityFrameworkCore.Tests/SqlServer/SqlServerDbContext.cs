using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.SqlServer;

public class SqlServerDbContext : DbContext
{
    public const string ConnectionString = "Server=localhost,1433;Database=weasel_testing;User Id=sa;Password=P@55w0rd;TrustServerCertificate=True";

    public SqlServerDbContext(DbContextOptions<SqlServerDbContext> options) : base(options)
    {
    }

    public DbSet<MyEntity> MyEntities => Set<MyEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MyEntity>(entity =>
        {
            entity.ToTable("MyEntities");
            entity.HasKey(e => e.Id);
        });
    }
}
