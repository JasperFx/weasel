using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     EF Core model sequences: UseHiLo declares a sequence (increment = block
///     size, 10 by default) and leaves the key column plain — no identity, no
///     default. Weasel must create the sequence with the same increment or
///     HiLo key generation would hand out colliding blocks.
/// </summary>
public class sequences
{
    public const string SchemaName = "efcmp_sequences";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new HiLoDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();

        var efSequence = result.EfSchema.SequenceFor("cmp_hilo")!;
        efSequence.IncrementBy.ShouldBe(10);

        var weaselSequence = result.WeaselSchema.SequenceFor("cmp_hilo")!;
        weaselSequence.IncrementBy.ShouldBe(10);
        weaselSequence.StartValue.ShouldBe(efSequence.StartValue);

        // HiLo keys are client-generated from the sequence: plain column
        var id = result.WeaselSchema.TableFor("HiLoItems")!.ColumnFor("Id")!;
        id.IsIdentity.ShouldBeFalse();
        id.DefaultExpression.ShouldBeNull();
    }
}

public class HiLoItem
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class HiLoDbContext : DbContext
{
    public DbSet<HiLoItem> Items => Set<HiLoItem>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(sequences.SchemaName);

        modelBuilder.Entity<HiLoItem>(entity =>
        {
            entity.ToTable("HiLoItems");
            NpgsqlPropertyBuilderExtensions.UseHiLo(entity.Property(e => e.Id), "cmp_hilo");
        });
    }
}
