using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.SqlServer.SchemaComparison;

/// <summary>
///     Baseline SQL Server conventions: IDENTITY(1,1) int keys, nvarchar(max)
///     strings, nvarchar(450) string keys (900-byte index key limit), the
///     conventional FK index, and PK naming.
/// </summary>
[Collection("sqlserver-schema-comparison")]
public class sqlserver_baseline
{
    public const string SchemaName = "efcmp_baseline";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new SqlBaselineDbContext();

        var result = await SchemaComparisonHarness.RunSqlServerAsync(context, SchemaName);

        result.AssertParity();

        var blogs = result.WeaselSchema.TableFor("CmpBlogs")!;
        blogs.ColumnFor("Id")!.IsIdentity.ShouldBeTrue();
        blogs.ColumnFor("Name")!.DataType.ShouldBe("nvarchar(max)");

        // string PK gets nvarchar(450) by convention
        result.WeaselSchema.TableFor("CmpCodes")!.ColumnFor("Code")!.DataType.ShouldBe("nvarchar(450)");

        result.WeaselSchema.TableFor("CmpPosts")!.IndexFor("IX_CmpPosts_BlogId").ShouldNotBeNull();
    }
}

/// <summary>
///     SQL Server default-value permutations, including the provider-specific
///     literal renderings (CAST(1 AS bit), N'...' unicode strings) and
///     getutcdate() default SQL.
/// </summary>
[Collection("sqlserver-schema-comparison")]
public class sqlserver_default_values
{
    public const string SchemaName = "efcmp_defaults";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new SqlDefaultsDbContext();

        var result = await SchemaComparisonHarness.RunSqlServerAsync(context, SchemaName);

        result.AssertParity();

        var table = result.EfSchema.TableFor("CmpSettings")!;
        table.ColumnFor("IntWithDefault")!.DefaultExpression.ShouldNotBeNull();
        table.ColumnFor("BoolWithDefault")!.DefaultExpression.ShouldContain("1");
        table.ColumnFor("StringWithDefault")!.DefaultExpression.ShouldContain("pending");
        table.ColumnFor("StampedAt")!.DefaultExpression!.ToLowerInvariant().ShouldContain("getutcdate");
    }
}

/// <summary>
///     SQL Server index permutations — most importantly the automatic
///     [Col] IS NOT NULL filter that SqlServerIndexConvention adds to every
///     unique index over nullable columns, plus explicit filters, included
///     columns, and descending-key metadata.
/// </summary>
[Collection("sqlserver-schema-comparison")]
public class sqlserver_indexes
{
    public const string SchemaName = "efcmp_indexes";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new SqlIndexesDbContext();

        var result = await SchemaComparisonHarness.RunSqlServerAsync(context, SchemaName);

        result.AssertParity();

        var table = result.EfSchema.TableFor("CmpDocs")!;

        // EF adds the filter automatically: unique index over a nullable column
        var uniqueNullable = table.IndexFor("IX_CmpDocs_ExternalId")!;
        uniqueNullable.IsUnique.ShouldBeTrue();
        uniqueNullable.Predicate.ShouldNotBeNull();
        uniqueNullable.Predicate.ShouldContain("IS NOT NULL");

        // ...and Weasel must have reproduced it
        result.WeaselSchema.TableFor("CmpDocs")!.IndexFor("IX_CmpDocs_ExternalId")!
            .Predicate.ShouldNotBeNull();

        table.IndexFor("IX_CmpDocs_Category_Covering")!.IncludedColumns.ShouldBe(["Title"]);
    }
}

/// <summary>
///     SQL Server rowversion concurrency tokens: byte[] + IsRowVersion maps to
///     the rowversion store type — a real column (unlike Npgsql's xmin).
/// </summary>
[Collection("sqlserver-schema-comparison")]
public class sqlserver_rowversion
{
    public const string SchemaName = "efcmp_rowver";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new SqlRowVersionDbContext();

        var result = await SchemaComparisonHarness.RunSqlServerAsync(context, SchemaName);

        result.AssertParity();

        result.EfSchema.TableFor("CmpVersioned")!.ColumnFor("Version")!.DataType.ShouldBe("timestamp");
        result.WeaselSchema.TableFor("CmpVersioned")!.ColumnFor("Version")!.DataType.ShouldBe("timestamp");
    }
}

/// <summary>
///     Delete behaviors on SQL Server: Restrict has no SQL Server equivalent
///     and must come out as NO ACTION — matching what EF's migrations emit.
/// </summary>
[Collection("sqlserver-schema-comparison")]
public class sqlserver_delete_behaviors
{
    public const string SchemaName = "efcmp_delactions";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new SqlDeleteBehaviorsDbContext();

        var result = await SchemaComparisonHarness.RunSqlServerAsync(context, SchemaName);

        result.AssertParity();

        var table = result.WeaselSchema.TableFor("CmpRefs")!;
        actionFor(table, "CascadeRefId").ShouldBe("CASCADE");
        actionFor(table, "SetNullRefId").ShouldBe("SET NULL");
        actionFor(table, "RestrictRefId").ShouldBe("NO ACTION");
        actionFor(table, "ClientSetNullRefId").ShouldBe("NO ACTION");
    }

    private static string actionFor(TableSnapshot table, string column)
        => table.ForeignKeys.Single(fk => fk.Columns.SequenceEqual([column])).OnDelete;
}

// ---- entities & contexts --------------------------------------------------

public class CmpBlog
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class CmpPost
{
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public int BlogId { get; set; }
    public CmpBlog Blog { get; set; } = null!;
}

public class CmpCode
{
    public string Code { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
}

public class SqlBaselineDbContext : DbContext
{
    public DbSet<CmpBlog> Blogs => Set<CmpBlog>();
    public DbSet<CmpPost> Posts => Set<CmpPost>();
    public DbSet<CmpCode> Codes => Set<CmpCode>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseSqlServer(SqlServerDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(sqlserver_baseline.SchemaName);
        modelBuilder.Entity<CmpBlog>().ToTable("CmpBlogs");
        modelBuilder.Entity<CmpPost>().ToTable("CmpPosts");
        modelBuilder.Entity<CmpCode>(entity =>
        {
            entity.ToTable("CmpCodes");
            entity.HasKey(e => e.Code);
        });
    }
}

public enum CmpLevel
{
    Low,
    High
}

public class CmpSetting
{
    public Guid Id { get; set; }
    public int IntWithDefault { get; set; }
    public bool BoolWithDefault { get; set; }
    public string StringWithDefault { get; set; } = string.Empty;
    public Guid GuidWithDefault { get; set; }
    public decimal DecimalWithDefault { get; set; }
    public CmpLevel EnumWithDefault { get; set; }
    public DateTime StampedAt { get; set; }
}

public class SqlDefaultsDbContext : DbContext
{
    public DbSet<CmpSetting> Settings => Set<CmpSetting>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseSqlServer(SqlServerDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(sqlserver_default_values.SchemaName);

        modelBuilder.Entity<CmpSetting>(entity =>
        {
            entity.ToTable("CmpSettings");
            entity.Property(e => e.IntWithDefault).HasDefaultValue(42);
            entity.Property(e => e.BoolWithDefault).HasDefaultValue(true);
            entity.Property(e => e.StringWithDefault).HasDefaultValue("pending");
            entity.Property(e => e.GuidWithDefault)
                .HasDefaultValue(new Guid("11111111-2222-3333-4444-555555555555"));
            entity.Property(e => e.DecimalWithDefault).HasPrecision(10, 2).HasDefaultValue(9.99m);
            entity.Property(e => e.EnumWithDefault).HasDefaultValue(CmpLevel.High);
            entity.Property(e => e.StampedAt).HasDefaultValueSql("getutcdate()");
        });
    }
}

public class CmpDoc
{
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public string? ExternalId { get; set; }
}

public class SqlIndexesDbContext : DbContext
{
    public DbSet<CmpDoc> Docs => Set<CmpDoc>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseSqlServer(SqlServerDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(sqlserver_indexes.SchemaName);

        modelBuilder.Entity<CmpDoc>(entity =>
        {
            entity.ToTable("CmpDocs");
            // unique over nullable -> EF auto-adds [ExternalId] IS NOT NULL filter
            entity.HasIndex(e => e.ExternalId).IsUnique();
            SqlServerIndexBuilderExtensions.IncludeProperties(
                entity.HasIndex(["Category"], "IX_CmpDocs_Category_Covering"),
                e => e.Title);
        });
    }
}

public class CmpVersionedRecord
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public byte[] Version { get; set; } = [];
}

public class SqlRowVersionDbContext : DbContext
{
    public DbSet<CmpVersionedRecord> Versioned => Set<CmpVersionedRecord>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseSqlServer(SqlServerDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(sqlserver_rowversion.SchemaName);

        modelBuilder.Entity<CmpVersionedRecord>(entity =>
        {
            entity.ToTable("CmpVersioned");
            entity.Property(e => e.Version).IsRowVersion();
        });
    }
}

// One principal table per FK: SQL Server rejects multiple potential cascade
// paths to the same target (EF's own create script fails otherwise)
public class CmpCascadeTarget
{
    public int Id { get; set; }
}

public class CmpSetNullTarget
{
    public int Id { get; set; }
}

public class CmpRestrictTarget
{
    public int Id { get; set; }
}

public class CmpClientTarget
{
    public int Id { get; set; }
}

public class CmpRef
{
    public int Id { get; set; }
    public int CascadeRefId { get; set; }
    public int? SetNullRefId { get; set; }
    public int RestrictRefId { get; set; }
    public int? ClientSetNullRefId { get; set; }
}

public class SqlDeleteBehaviorsDbContext : DbContext
{
    public DbSet<CmpCascadeTarget> CascadeTargets => Set<CmpCascadeTarget>();
    public DbSet<CmpSetNullTarget> SetNullTargets => Set<CmpSetNullTarget>();
    public DbSet<CmpRestrictTarget> RestrictTargets => Set<CmpRestrictTarget>();
    public DbSet<CmpClientTarget> ClientTargets => Set<CmpClientTarget>();
    public DbSet<CmpRef> Refs => Set<CmpRef>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseSqlServer(SqlServerDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(sqlserver_delete_behaviors.SchemaName);

        modelBuilder.Entity<CmpCascadeTarget>().ToTable("CmpCascadeTargets");
        modelBuilder.Entity<CmpSetNullTarget>().ToTable("CmpSetNullTargets");
        modelBuilder.Entity<CmpRestrictTarget>().ToTable("CmpRestrictTargets");
        modelBuilder.Entity<CmpClientTarget>().ToTable("CmpClientTargets");
        modelBuilder.Entity<CmpRef>(entity =>
        {
            entity.ToTable("CmpRefs");
            entity.HasOne<CmpCascadeTarget>().WithMany().HasForeignKey(e => e.CascadeRefId)
                .OnDelete(DeleteBehavior.Cascade);
            entity.HasOne<CmpSetNullTarget>().WithMany().HasForeignKey(e => e.SetNullRefId)
                .OnDelete(DeleteBehavior.SetNull);
            entity.HasOne<CmpRestrictTarget>().WithMany().HasForeignKey(e => e.RestrictRefId)
                .OnDelete(DeleteBehavior.Restrict);
            entity.HasOne<CmpClientTarget>().WithMany().HasForeignKey(e => e.ClientSetNullRefId)
                .OnDelete(DeleteBehavior.ClientSetNull);
        });
    }
}
