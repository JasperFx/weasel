using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Weasel.Core;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     EF Core check constraints (ToTable(t =&gt; t.HasCheckConstraint(...)))
///     map into Weasel table definitions, are emitted in CREATE TABLE, and
///     participate in delta detection — conservatively: only declared checks
///     are compared, and unknown constraints in the database are never dropped.
/// </summary>
public class check_constraints
{
    public const string SchemaName = "efcmp_checks";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new CheckConstraintDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();

        result.EfSchema.TableFor("PricedItems")!.CheckConstraints
            .ShouldContain(c => c.Name == "CK_PricedItems_Price");
        result.WeaselSchema.TableFor("PricedItems")!.CheckConstraints
            .ShouldContain(c => c.Name == "CK_PricedItems_Price");

        result.DeltaAgainstEfSchema.ShouldBe(SchemaPatchDifference.None);
    }
}

/// <summary>
///     Computed / generated columns: HasComputedColumnSql maps into Weasel and
///     is emitted as GENERATED ALWAYS AS (...) STORED on PostgreSQL (the only
///     kind PostgreSQL supports).
/// </summary>
public class computed_columns
{
    public const string SchemaName = "efcmp_computed";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new ComputedColumnDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();

        result.EfSchema.TableFor("People")!.ColumnFor("FullName")!.IsComputed.ShouldBeTrue();
        result.WeaselSchema.TableFor("People")!.ColumnFor("FullName")!.IsComputed.ShouldBeTrue();
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

public class ComputedPerson
{
    public int Id { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string FullName { get; set; } = string.Empty;
}

public class ComputedColumnDbContext : DbContext
{
    public DbSet<ComputedPerson> People => Set<ComputedPerson>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(computed_columns.SchemaName);

        modelBuilder.Entity<ComputedPerson>(entity =>
        {
            entity.ToTable("People");
            // PostgreSQL only supports stored generated columns
            entity.Property(e => e.FullName)
                .HasComputedColumnSql("\"FirstName\" || ' ' || \"LastName\"", stored: true);
        });
    }
}
