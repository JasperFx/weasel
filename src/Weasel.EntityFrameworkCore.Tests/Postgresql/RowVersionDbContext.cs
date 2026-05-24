using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

/// <summary>
///     Models the Npgsql-recommended optimistic-concurrency pattern: a <c>uint</c>
///     property mapped via <c>IsRowVersion()</c>, which Npgsql maps to PostgreSQL's
///     implicit <c>xmin</c> system column rather than creating a new column.
///     Regression coverage for weasel#290.
/// </summary>
public class RowVersionDbContext: DbContext
{
    public const string ConnectionString =
        "Host=localhost;Port=5432;Database=marten_testing;Username=postgres;Password=postgres";

    public RowVersionDbContext(DbContextOptions<RowVersionDbContext> options): base(options)
    {
    }

    public DbSet<VersionedRecord> Records => Set<VersionedRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<VersionedRecord>(entity =>
        {
            entity.ToTable("versioned_records", "ef_rowversion_test");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(100);
            entity.Property(e => e.Version).IsRowVersion(); // Npgsql → xmin system column
        });
    }
}

public class VersionedRecord
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public uint Version { get; set; }
}
