using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

public class PostgresqlDbContext : DbContext
{
    /// <summary>
    ///     The one connection string for the Postgres half of this suite. Honors
    ///     <c>weasel_postgresql_testing_database</c> like every other Weasel suite does — until
    ///     weasel#627 this was a hard-coded const that no environment variable reached, so a
    ///     developer with Postgres on any other port could not run these tests at all (the same
    ///     trap weasel#620 closed on the SQL Server half).
    /// </summary>
    public static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("weasel_postgresql_testing_database")
        ?? "Host=localhost;Port=5432;Database=marten_testing;Username=postgres;Password=postgres";

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
