using FirebirdSql.Data.FirebirdClient;
using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.Firebird;

public class FirebirdDbContext : DbContext
{
    // Honours the same environment variable as Weasel.Firebird.Tests' ConnectionSource, so one setting
    // points both suites at the same server; the literal is docker-compose's Firebird 5. The file name
    // is replaced, because these tests drop and recreate their database, and that must never be the
    // file another suite is using.
    public static readonly string ConnectionString = ownDatabase(
        Environment.GetEnvironmentVariable("weasel_firebird_testing_database")
        ?? "DataSource=localhost;Port=3065;Database=/var/lib/firebird/data/weasel_testing.fdb;User=SYSDBA;Password=P@55w0rd;Charset=UTF8");

    public FirebirdDbContext(DbContextOptions<FirebirdDbContext> options) : base(options)
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

    private static string ownDatabase(string connectionString)
    {
        var builder = new FbConnectionStringBuilder(connectionString);
        var path = builder.Database;
        var separator = Math.Max(path.LastIndexOf('/'), path.LastIndexOf('\\'));
        builder.Database = $"{path[..(separator + 1)]}weasel_efcore.fdb";

        return builder.ConnectionString;
    }
}
