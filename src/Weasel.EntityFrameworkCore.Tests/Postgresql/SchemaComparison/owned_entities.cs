using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     Owned entity permutations: OwnsOne table splitting (Nav_Prop columns in
///     the owner's table, no FK constraint), OwnsOne with ToTable (separate
///     table, PK == FK to owner, cascade), and OwnsMany (separate table with
///     composite (OwnerId, Id) PK).
/// </summary>
public class owned_entities
{
    public const string SchemaName = "efcmp_owned";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new OwnedEntitiesDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();

        // table splitting: owned columns in the owner's table, prefixed Nav_Prop
        var orders = result.WeaselSchema.TableFor("OwnedOrders")!;
        orders.ColumnFor("ShippingAddress_Street").ShouldNotBeNull();
        orders.ColumnFor("ShippingAddress_City").ShouldNotBeNull();
        // optional owned type -> all its columns nullable
        orders.ColumnFor("ShippingAddress_Street")!.IsNullable.ShouldBeTrue();
        // no row-internal FK constraint
        orders.ForeignKeys.ShouldBeEmpty();

        // OwnsOne + ToTable: separate table, PK == FK, cascade, no FK index
        var billing = result.WeaselSchema.TableFor("BillingAddresses")!;
        billing.PrimaryKeyColumns.ShouldBe(["OwnedOrderId"]);
        billing.ForeignKeys.Single().OnDelete.ShouldBe("CASCADE");
        billing.Indexes.Where(i => !i.IsPrimaryKey).ShouldBeEmpty();

        // OwnsMany: separate table, composite (OwnerId, synthesized Id) PK
        var lines = result.WeaselSchema.TableFor("OrderLines")!;
        lines.PrimaryKeyColumns.ShouldBe(["OwnedOrderId", "Id"]);
        lines.ForeignKeys.Single().OnDelete.ShouldBe("CASCADE");
    }
}

public class OwnedOrder
{
    public int Id { get; set; }
    public string Number { get; set; } = string.Empty;
    public OwnedAddress? ShippingAddress { get; set; }
    public OwnedAddress? BillingAddress { get; set; }
    public List<OwnedOrderLine> Lines { get; set; } = [];
}

public class OwnedAddress
{
    public string Street { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
}

public class OwnedOrderLine
{
    public string Sku { get; set; } = string.Empty;
    public int Quantity { get; set; }
}

public class OwnedEntitiesDbContext : DbContext
{
    public DbSet<OwnedOrder> Orders => Set<OwnedOrder>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(owned_entities.SchemaName);

        modelBuilder.Entity<OwnedOrder>(entity =>
        {
            entity.ToTable("OwnedOrders");
            entity.OwnsOne(e => e.ShippingAddress);
            entity.OwnsOne(e => e.BillingAddress, owned => owned.ToTable("BillingAddresses"));
            entity.OwnsMany(e => e.Lines, owned => owned.ToTable("OrderLines"));
        });
    }
}
