using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Weasel.Core;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     KNOWN GAP, documented by this test: EF Core check constraints
///     (ToTable(t =&gt; t.HasCheckConstraint(...))) are not modeled by Weasel —
///     ITable has no check-constraint surface and neither provider's
///     FetchExisting reads them. The Weasel-created schema therefore lacks
///     them (tolerated MissingCheckConstraint), while Weasel's delta remains
///     None because check constraints are invisible to its comparison.
/// </summary>
public class check_constraints
{
    public const string SchemaName = "efcmp_checks";

    [Fact]
    public async Task check_constraints_are_a_documented_gap()
    {
        await using var context = new CheckConstraintDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity(DifferenceCategory.MissingCheckConstraint);

        // the EF side really has it...
        result.EfSchema.TableFor("PricedItems")!.CheckConstraints
            .ShouldContain(c => c.Name == "CK_PricedItems_Price");
        // ...and the Weasel side really lacks it
        result.WeaselSchema.TableFor("PricedItems")!.CheckConstraints.ShouldBeEmpty();

        // Weasel cannot see check constraints, so its delta stays None either way
        result.DeltaAgainstEfSchema.ShouldBe(SchemaPatchDifference.None);
    }
}

public class PricedItem
{
    public int Id { get; set; }
    public decimal Price { get; set; }
}

public class CheckConstraintDbContext : DbContext
{
    public DbSet<PricedItem> PricedItems => Set<PricedItem>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(check_constraints.SchemaName);

        modelBuilder.Entity<PricedItem>(entity =>
        {
            entity.ToTable("PricedItems", t => t.HasCheckConstraint("CK_PricedItems_Price", "\"Price\" > 0"));
        });
    }
}
