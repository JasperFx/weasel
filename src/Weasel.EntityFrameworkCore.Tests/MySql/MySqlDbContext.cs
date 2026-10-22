using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.MySql;

public class MySqlDbContext : DbContext
{
    // The same variable Weasel.MySql.Tests reads, so the suite can reach a MySQL that is not on
    // localhost:3306 (a shared machine, the local gate: weasel#733).
    public static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("weasel_mysql_testing_database")
        ?? "Server=localhost;Port=3306;Database=weasel_testing;User=weasel;Password=P@55w0rd";

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
