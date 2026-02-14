using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.Oracle;

public class OracleDbContext : DbContext
{
    public const string ConnectionString = "User Id=weasel;Password=P@55w0rd;Data Source=localhost:1521/FREEPDB1";

    public OracleDbContext(DbContextOptions<OracleDbContext> options) : base(options)
    {
    }

    public DbSet<MyEntity> MyEntities => Set<MyEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MyEntity>(entity =>
        {
            entity.ToTable("MY_ENTITIES");
            entity.HasKey(e => e.Id);

            // Oracle has 30 character limit for identifiers in older versions
            // Using shorter column names for compatibility
            entity.Property(e => e.DateTimeOffsetValue).HasColumnName("DT_OFFSET_VAL");
            entity.Property(e => e.NullableDateTimeOffsetValue).HasColumnName("NULL_DT_OFFSET_VAL");
            entity.Property(e => e.NullableCascadeActionValue).HasColumnName("NULL_CASCADE_VAL");
            entity.Property(e => e.CascadeActionValue).HasColumnName("CASCADE_VAL");
            entity.Property(e => e.NullableDateTimeValue).HasColumnName("NULL_DT_VAL");
            entity.Property(e => e.NullableDateOnlyValue).HasColumnName("NULL_DATE_VAL");
            entity.Property(e => e.NullableTimeOnlyValue).HasColumnName("NULL_TIME_VAL");
            entity.Property(e => e.NullableGuidValue).HasColumnName("NULL_GUID_VAL");
            entity.Property(e => e.NullableBoolValue).HasColumnName("NULL_BOOL_VAL");
            entity.Property(e => e.NullableIntValue).HasColumnName("NULL_INT_VAL");
        });
    }
}
