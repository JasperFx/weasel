using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     TPH parity: single table with a sized discriminator (EF8+ gives the
///     discriminator a max length covering its known values), derived-type
///     columns forced nullable, and a derived-type FK + index folded into the
///     hierarchy table.
/// </summary>
public class inheritance_tph
{
    public const string SchemaName = "efcmp_tph";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new TphComparisonDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();

        var table = result.WeaselSchema.TableFor("Shapes")!;
        table.ColumnFor("Discriminator")!.IsNullable.ShouldBeFalse();
        // required properties of derived types map to nullable columns
        table.ColumnFor("Radius")!.IsNullable.ShouldBeTrue();
        table.ColumnFor("SideLength")!.IsNullable.ShouldBeTrue();
        // derived-type FK and its conventional index live on the hierarchy table
        table.ForeignKeys.Single().Columns.ShouldBe(["PaletteId"]);
        table.IndexFor("IX_Shapes_PaletteId").ShouldNotBeNull();
    }
}

/// <summary>
///     TPT parity: one table per type, the derived tables keyed by a PK that is
///     also an FK to the base table, and no conventional index for that FK
///     (PK-prefix covered).
/// </summary>
public class inheritance_tpt
{
    public const string SchemaName = "efcmp_tpt";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new TptComparisonDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();

        result.WeaselSchema.TableFor("Vehicles").ShouldNotBeNull();
        var cars = result.WeaselSchema.TableFor("Cars")!;
        cars.PrimaryKeyColumns.ShouldBe(["Id"]);
        cars.ForeignKeys.Single().PrincipalTable.ShouldBe("Vehicles");
        // base-only columns must not leak into the derived tables
        cars.ColumnFor("Wheels").ShouldBeNull();

        var trucks = result.WeaselSchema.TableFor("Trucks")!;
        trucks.ForeignKeys.Single().PrincipalTable.ShouldBe("Vehicles");
    }
}

public abstract class TphShape
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int? PaletteId { get; set; }
    public TphPalette? Palette { get; set; }
}

public class TphCircle : TphShape
{
    public double Radius { get; set; }
}

public class TphSquare : TphShape
{
    public double SideLength { get; set; }
}

public class TphPalette
{
    public int Id { get; set; }
    public string Colors { get; set; } = string.Empty;
}

public class TphComparisonDbContext : DbContext
{
    public DbSet<TphShape> Shapes => Set<TphShape>();
    public DbSet<TphPalette> Palettes => Set<TphPalette>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(inheritance_tph.SchemaName);

        modelBuilder.Entity<TphShape>(entity =>
        {
            entity.ToTable("Shapes");
            entity.HasDiscriminator<string>("Discriminator")
                .HasValue<TphCircle>("circle")
                .HasValue<TphSquare>("square");
        });
    }
}

public abstract class TptVehicle
{
    public int Id { get; set; }
    public int Wheels { get; set; }
}

public class TptCar : TptVehicle
{
    public string Trim { get; set; } = string.Empty;
}

public class TptTruck : TptVehicle
{
    public decimal PayloadTons { get; set; }
}

public class TptComparisonDbContext : DbContext
{
    public DbSet<TptVehicle> Vehicles => Set<TptVehicle>();
    public DbSet<TptCar> Cars => Set<TptCar>();
    public DbSet<TptTruck> Trucks => Set<TptTruck>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(inheritance_tpt.SchemaName);

        modelBuilder.Entity<TptVehicle>().ToTable("Vehicles");
        modelBuilder.Entity<TptCar>().ToTable("Cars");
        modelBuilder.Entity<TptTruck>().ToTable("Trucks");
    }
}
