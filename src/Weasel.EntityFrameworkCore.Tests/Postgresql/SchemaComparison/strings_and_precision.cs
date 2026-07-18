using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     Column facet permutations: max length, fixed length, decimal
///     precision/scale, temporal precision, and collation-free defaults.
/// </summary>
public class strings_and_precision
{
    public const string SchemaName = "efcmp_facets";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new FacetsDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();

        var table = result.WeaselSchema.TableFor("FacetItems")!;
        table.ColumnFor("BoundedName")!.DataType.ShouldBe("varchar(200)");
        table.ColumnFor("FixedCode")!.DataType.ShouldBe("char(8)");
        table.ColumnFor("Price")!.DataType.ShouldBe("numeric(18,4)");
        table.ColumnFor("LoggedAt")!.DataType.ShouldStartWith("timestamp(3)");
        table.ColumnFor("UnboundedText")!.DataType.ShouldBe("text");
    }
}

public class FacetItem
{
    public int Id { get; set; }
    public string BoundedName { get; set; } = string.Empty;
    public string FixedCode { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public DateTime LoggedAt { get; set; }
    public string UnboundedText { get; set; } = string.Empty;
}

public class FacetsDbContext : DbContext
{
    public DbSet<FacetItem> FacetItems => Set<FacetItem>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(strings_and_precision.SchemaName);

        modelBuilder.Entity<FacetItem>(entity =>
        {
            entity.ToTable("FacetItems");
            entity.Property(e => e.BoundedName).HasMaxLength(200);
            entity.Property(e => e.FixedCode).HasMaxLength(8).IsFixedLength();
            entity.Property(e => e.Price).HasPrecision(18, 4);
            entity.Property(e => e.LoggedAt).HasPrecision(3);
        });
    }
}
