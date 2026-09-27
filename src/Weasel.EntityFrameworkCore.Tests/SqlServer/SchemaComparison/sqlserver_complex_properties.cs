using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.SqlServer.SchemaComparison;

/// <summary>
///     The SQL Server half of weasel#628: table-split <c>ComplexProperty</c> members become columns
///     of the owner's own table here too, and the mapper skipped them on every provider.
/// </summary>
[Collection("sqlserver-schema-comparison")]
public class sqlserver_complex_properties
{
    public const string SchemaName = "efcmp_complex";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new SqlComplexPropertyDbContext();

        var result = await SchemaComparisonHarness.RunSqlServerAsync(context, SchemaName);

        result.AssertParity();

        var orders = result.WeaselSchema.TableFor("CmpComplexOrders")!;

        orders.ColumnFor("Total_Amount").ShouldNotBeNull();
        orders.ColumnFor("Total_Currency").ShouldNotBeNull();
        orders.ColumnFor("Total_Amount")!.IsNullable.ShouldBeFalse();

        // the precision the complex member declares survives the extra hop
        orders.ColumnFor("Total_Amount")!.DataType.ShouldBe("decimal(18,4)");

        // nested complex properties are walked transitively
        orders.ColumnFor("Origin_City").ShouldNotBeNull();
        orders.ColumnFor("Origin_Coordinates_Latitude").ShouldNotBeNull();

        orders.ForeignKeys.ShouldBeEmpty();
    }
}

public class CmpComplexOrder
{
    public int Id { get; set; }
    public string Number { get; set; } = string.Empty;
    public CmpMoney Total { get; set; } = null!;
    public CmpOrigin Origin { get; set; } = null!;
}

public class CmpMoney
{
    public decimal Amount { get; set; }
    public string Currency { get; set; } = string.Empty;
}

public class CmpOrigin
{
    public string City { get; set; } = string.Empty;
    public CmpCoordinates Coordinates { get; set; } = null!;
}

public class CmpCoordinates
{
    public double Latitude { get; set; }
    public double Longitude { get; set; }
}

public class SqlComplexPropertyDbContext : DbContext
{
    public DbSet<CmpComplexOrder> Orders => Set<CmpComplexOrder>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseSqlServer(SqlServerDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(sqlserver_complex_properties.SchemaName);

        modelBuilder.Entity<CmpComplexOrder>(entity =>
        {
            entity.ToTable("CmpComplexOrders");

            entity.ComplexProperty(e => e.Total,
                c => c.Property(m => m.Amount).HasColumnType("decimal(18,4)"));

            entity.ComplexProperty(e => e.Origin, c => c.ComplexProperty(o => o.Coordinates));
        });
    }
}
