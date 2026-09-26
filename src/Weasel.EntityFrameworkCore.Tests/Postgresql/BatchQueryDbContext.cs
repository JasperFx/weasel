using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

/// <summary>
///     An aggregate with every shape EF Core materializes beyond flat columns, for checking that
///     <see cref="Batching.BatchedQuery" /> returns what EF Core returns.
/// </summary>
public class BatchQueryDbContext: DbContext
{
    public const string ConnectionString =
        "Host=localhost;Port=5432;Database=marten_testing;Username=postgres;Password=postgres";

    public const string TestSchema = "batch_query_test";

    public BatchQueryDbContext(DbContextOptions<BatchQueryDbContext> options): base(options)
    {
    }

    public DbSet<BatchOrder> Orders => Set<BatchOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(TestSchema);

        modelBuilder.Entity<BatchOrder>(entity =>
        {
            entity.ToTable("orders");
            entity.OwnsOne(x => x.ShippingAddress);
            entity.OwnsOne(x => x.Settings, s => s.ToJson());
            entity.ComplexProperty(x => x.Total);
#if NET10_0_OR_GREATER
            entity.ComplexProperty(x => x.Discount, d => d.ToJson());
            entity.ComplexCollection(x => x.Tags, t => t.ToJson());
#else
            entity.Ignore(x => x.Discount);
            entity.Ignore(x => x.Tags);
#endif
            entity.HasMany(x => x.Lines).WithOne().HasForeignKey(x => x.OrderId);
        });

        modelBuilder.Entity<BatchOrderLine>().ToTable("order_lines");
    }
}

public class BatchOrder
{
    // No public parameterless constructor, like most aggregates
    private BatchOrder()
    {
    }

    public BatchOrder(Guid id, string customer)
    {
        Id = id;
        Customer = customer;
    }

    public Guid Id { get; private set; }
    public string Customer { get; private set; } = "";
    public BatchAddress ShippingAddress { get; set; } = null!; // owned type
    public BatchSettings Settings { get; set; } = null!; // owned type in a JSON column
    public BatchMoney Total { get; set; } = null!; // complex type
    public BatchMoney? Discount { get; set; } // optional complex type in a JSON column (EF Core 10)
    public List<BatchTag> Tags { get; set; } = []; // complex collection in a JSON column (EF Core 10)
    public List<BatchOrderLine> Lines { get; set; } = []; // navigation
}

public class BatchAddress
{
    public string City { get; set; } = "";
}

public class BatchSettings
{
    public bool Gift { get; set; }
}

public class BatchMoney
{
    public decimal Amount { get; set; }
}

public class BatchTag
{
    public string Name { get; set; } = "";
}

public class BatchOrderLine
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public string Sku { get; set; } = "";
}
