using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     Table-split <c>ComplexProperty</c> members: EF Core 8+ complex types that are NOT
///     <c>ToJson()</c>, whose scalars become columns of the owner's own table exactly as a
///     table-split <c>OwnsOne</c>'s do. They are not navigations and carry no entity type of their
///     own, so <c>MapToTable</c> reached them through neither of its two walks — the only code
///     looking at <c>GetComplexProperties()</c> was the <c>ToJson()</c> container mapping from
///     weasel#291, which skipped everything else.
///     <para>
///     Two failures came out of that one gap: a table created without the columns, whose first EF
///     insert fails with <c>42703</c>, and — against a table EF Core itself created — a
///     <c>CreateOrUpdate</c> delta that DROPPED them (weasel#628).
///     </para>
/// </summary>
public class complex_properties
{
    public const string SchemaName = "efcmp_complex";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new ComplexPropertyDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        // AssertParity covers the whole defect: catalog parity with EF's own DDL (the missing
        // columns) AND a None delta against the EF-created schema (the drops)
        result.AssertParity();

        var orders = result.WeaselSchema.TableFor("ComplexOrders")!;

        // a required complex property contributes NOT NULL columns to the owner's table
        orders.ColumnFor("Total_Amount").ShouldNotBeNull();
        orders.ColumnFor("Total_Currency").ShouldNotBeNull();
        orders.ColumnFor("Total_Amount")!.IsNullable.ShouldBeFalse();

        // an explicit HasColumnName on a complex member is honored -- the shape
        // UseSnakeCaseNamingConvention() produces, and the one whose drop executes silently
        orders.ColumnFor("shipping_fee").ShouldNotBeNull();
        orders.ColumnFor("shipping_currency").ShouldNotBeNull();

        // nested complex properties are walked transitively
        orders.ColumnFor("Origin_City").ShouldNotBeNull();
        orders.ColumnFor("Origin_Coordinates_Latitude").ShouldNotBeNull();
        orders.ColumnFor("Origin_Coordinates_Longitude").ShouldNotBeNull();

        // a complex member called "Id" is a plain column, not a second primary key
        orders.ColumnFor("Origin_Id").ShouldNotBeNull();
        orders.PrimaryKeyColumns.ShouldBe(["Id"]);

        // no row-internal FK constraint: a complex type is part of the row, not a relationship
        orders.ForeignKeys.ShouldBeEmpty();
    }

#if NET10_0_OR_GREATER
    /// <summary>
    ///     A table-split complex property and a <c>ToJson()</c> one on the same entity: the first
    ///     contributes a column per member, the second a single container column. weasel#291
    ///     covered the JSON half on its own; this pins that adding the table-split walk did not
    ///     make the two collide in <c>addedColumns</c>.
    /// </summary>
    [Fact]
    public async Task table_split_and_json_complex_properties_on_the_same_entity()
    {
        await using var context = new MixedComplexPropertyDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, MixedComplexPropertyDbContext.SchemaName);

        result.AssertParity();

        var receipts = result.WeaselSchema.TableFor("MixedReceipts")!;
        receipts.ColumnFor("Paid_Amount").ShouldNotBeNull();
        receipts.ColumnFor("Paid_Currency").ShouldNotBeNull();
        receipts.ColumnFor("audit").ShouldNotBeNull();

        // the JSON one is a single container column, not a column per member
        receipts.ColumnFor("Audit_Amount").ShouldBeNull();
    }
#endif
}

public class ComplexOrder
{
    public int Id { get; set; }
    public string Number { get; set; } = string.Empty;
    public ComplexMoney Total { get; set; } = null!;
    public ComplexMoney Shipping { get; set; } = null!;
    public ComplexOrigin Origin { get; set; } = null!;
}

public class ComplexMoney
{
    public decimal Amount { get; set; }
    public string Currency { get; set; } = string.Empty;
}

public class ComplexOrigin
{
    /// <summary>
    ///     Deliberately called Id: a complex member's name is its name within the complex type, so
    ///     matching it against the entity's primary-key property names would mark this column as
    ///     part of the PK.
    /// </summary>
    public int Id { get; set; }

    public string City { get; set; } = string.Empty;
    public ComplexCoordinates Coordinates { get; set; } = null!;
}

public class ComplexCoordinates
{
    public double Latitude { get; set; }
    public double Longitude { get; set; }
}

public class ComplexPropertyDbContext : DbContext
{
    public DbSet<ComplexOrder> Orders => Set<ComplexOrder>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(complex_properties.SchemaName);

        modelBuilder.Entity<ComplexOrder>(entity =>
        {
            entity.ToTable("ComplexOrders");

            // conventional Nav_Prop column names
            entity.ComplexProperty(e => e.Total);

            // explicit column names, the shape UseSnakeCaseNamingConvention() produces
            entity.ComplexProperty(e => e.Shipping, c =>
            {
                c.Property(m => m.Amount).HasColumnName("shipping_fee");
                c.Property(m => m.Currency).HasColumnName("shipping_currency");
            });

            // nested one level deeper. Every complex property here is required: EF Core 9 refuses
            // to model an optional one at all ("Configuring the complex property ... as optional
            // is not supported", dotnet/efcore#31376), so there is no optional case to cover
            // across both target frameworks.
            entity.ComplexProperty(e => e.Origin, c => c.ComplexProperty(o => o.Coordinates));
        });
    }
}

#if NET10_0_OR_GREATER
public class MixedReceipt
{
    public int Id { get; set; }
    public ComplexMoney Paid { get; set; } = null!;
    public ComplexMoney Audit { get; set; } = null!;
}

public class MixedComplexPropertyDbContext : DbContext
{
    public const string SchemaName = "efcmp_complex_mixed";

    public DbSet<MixedReceipt> Receipts => Set<MixedReceipt>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(SchemaName);

        modelBuilder.Entity<MixedReceipt>(entity =>
        {
            entity.ToTable("MixedReceipts");
            entity.ComplexProperty(e => e.Paid);
            entity.ComplexProperty(e => e.Audit, c => c.ToJson("audit"));
        });
    }
}
#endif
