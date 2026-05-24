#if NET10_0_OR_GREATER
using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

/// <summary>
///     EF Core 10 <c>ComplexProperty(...).ToJson()</c> and
///     <c>ComplexCollection(...).ToJson()</c> mappings — value-object aggregates
///     serialized into a single JSONB container column each. Unlike
///     <c>OwnsOne(...).ToJson()</c>, complex properties are not navigations and have
///     no separate entity type. Regression coverage for weasel#291.
/// </summary>
public class ComplexJsonDbContext: DbContext
{
    public const string ConnectionString =
        "Host=localhost;Port=5432;Database=marten_testing;Username=postgres;Password=postgres";

    public ComplexJsonDbContext(DbContextOptions<ComplexJsonDbContext> options): base(options)
    {
    }

    public DbSet<Order> Orders => Set<Order>();
    public DbSet<Invoice> Invoices => Set<Invoice>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("orders", "ef_complex_test");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.ComplexProperty(e => e.Shipping, c => c.ToJson("shipping"));
        });

        modelBuilder.Entity<Invoice>(entity =>
        {
            entity.ToTable("invoices", "ef_complex_test");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.ComplexCollection(e => e.Lines, c => c.ToJson("lines"));
        });
    }
}

public class Order
{
    public Guid Id { get; set; }
    public Address Shipping { get; set; } = new();
}

public class Address
{
    public string Street { get; set; } = "";
    public string City { get; set; } = "";
}

public class Invoice
{
    public Guid Id { get; set; }
    public List<InvoiceLine> Lines { get; init; } = [];
}

public class InvoiceLine
{
    public string Sku { get; set; } = "";
    public decimal Amount { get; set; }
}
#endif
