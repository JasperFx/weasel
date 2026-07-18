using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     Column default permutations: literal HasDefaultValue over the scalar
///     type matrix (including enum and enum-converted-to-string literals),
///     HasDefaultValueSql, and defaults on nullable columns.
/// </summary>
public class default_values
{
    public const string SchemaName = "efcmp_defaults";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new DefaultValuesDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();

        // The EF-created schema must actually carry the defaults we think we
        // are comparing — guard against the comparison passing vacuously.
        var table = result.EfSchema.TableFor("Settings")!;
        table.ColumnFor("IntWithDefault")!.DefaultExpression.ShouldNotBeNull();
        table.ColumnFor("BoolWithDefault")!.DefaultExpression.ShouldNotBeNull();
        table.ColumnFor("StringWithDefault")!.DefaultExpression.ShouldNotBeNull();
        table.ColumnFor("GuidWithDefault")!.DefaultExpression.ShouldNotBeNull();
        table.ColumnFor("DateTimeWithDefault")!.DefaultExpression.ShouldNotBeNull();
        table.ColumnFor("DecimalWithDefault")!.DefaultExpression.ShouldNotBeNull();
        table.ColumnFor("EnumWithDefault")!.DefaultExpression.ShouldNotBeNull();
        table.ColumnFor("EnumStringWithDefault")!.DefaultExpression.ShouldNotBeNull();
        table.ColumnFor("CreatedAt")!.DefaultExpression.ShouldContain("now()");
        table.ColumnFor("NullableIntWithDefault")!.DefaultExpression.ShouldNotBeNull();
    }
}

public enum SettingLevel
{
    Low = 0,
    Medium = 1,
    High = 2
}

public class SettingEntity
{
    public Guid Id { get; set; }
    public int IntWithDefault { get; set; }
    public bool BoolWithDefault { get; set; }
    public string StringWithDefault { get; set; } = string.Empty;
    public Guid GuidWithDefault { get; set; }
    public DateTime DateTimeWithDefault { get; set; }
    public decimal DecimalWithDefault { get; set; }
    public SettingLevel EnumWithDefault { get; set; }
    public SettingLevel EnumStringWithDefault { get; set; }
    public DateTime CreatedAt { get; set; }
    public int? NullableIntWithDefault { get; set; }
    public string NoDefault { get; set; } = string.Empty;
}

public class DefaultValuesDbContext : DbContext
{
    public DbSet<SettingEntity> Settings => Set<SettingEntity>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(default_values.SchemaName);

        modelBuilder.Entity<SettingEntity>(entity =>
        {
            entity.ToTable("Settings");
            entity.Property(e => e.IntWithDefault).HasDefaultValue(42);
            entity.Property(e => e.BoolWithDefault).HasDefaultValue(true);
            entity.Property(e => e.StringWithDefault).HasDefaultValue("pending");
            entity.Property(e => e.GuidWithDefault)
                .HasDefaultValue(new Guid("11111111-2222-3333-4444-555555555555"));
            entity.Property(e => e.DateTimeWithDefault)
                .HasDefaultValue(new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc));
            entity.Property(e => e.DecimalWithDefault)
                .HasPrecision(10, 2)
                .HasDefaultValue(9.99m);
            entity.Property(e => e.EnumWithDefault).HasDefaultValue(SettingLevel.Medium);
            entity.Property(e => e.EnumStringWithDefault)
                .HasConversion<string>()
                .HasMaxLength(20)
                .HasDefaultValue(SettingLevel.High);
            entity.Property(e => e.CreatedAt).HasDefaultValueSql("now()");
            entity.Property(e => e.NullableIntWithDefault).HasDefaultValue(7);
        });
    }
}
