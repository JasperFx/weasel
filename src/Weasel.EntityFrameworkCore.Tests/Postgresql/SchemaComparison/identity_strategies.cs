using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     Value-generation permutations on Npgsql: the conventional
///     identity-by-default int key, identity-always, ValueGeneratedNever,
///     long/short key sizes, and client-generated Guid keys (no DDL
///     generation at all).
/// </summary>
public class identity_strategies
{
    public const string SchemaName = "efcmp_identity";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new IdentityStrategiesDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();

        result.WeaselSchema.TableFor("IdConvention")!.ColumnFor("Id")!.IsIdentity.ShouldBeTrue();
        result.WeaselSchema.TableFor("IdAlways")!.ColumnFor("Id")!.IsIdentity.ShouldBeTrue();
        result.WeaselSchema.TableFor("IdNever")!.ColumnFor("Id")!.IsIdentity.ShouldBeFalse();
        result.WeaselSchema.TableFor("IdLong")!.ColumnFor("Id")!.DataType.ShouldBe("bigint");
        result.WeaselSchema.TableFor("IdLong")!.ColumnFor("Id")!.IsIdentity.ShouldBeTrue();
        // Guid keys are client-generated: no identity, no default
        var guidId = result.WeaselSchema.TableFor("IdGuid")!.ColumnFor("Id")!;
        guidId.IsIdentity.ShouldBeFalse();
        guidId.DefaultExpression.ShouldBeNull();
    }
}

public class IdConventionEntity
{
    public int Id { get; set; }
}

public class IdAlwaysEntity
{
    public int Id { get; set; }
}

public class IdNeverEntity
{
    public int Id { get; set; }
}

public class IdLongEntity
{
    public long Id { get; set; }
}

public class IdGuidEntity
{
    public Guid Id { get; set; }
}

public class IdentityStrategiesDbContext : DbContext
{
    public DbSet<IdConventionEntity> Conventions => Set<IdConventionEntity>();
    public DbSet<IdAlwaysEntity> Always => Set<IdAlwaysEntity>();
    public DbSet<IdNeverEntity> Nevers => Set<IdNeverEntity>();
    public DbSet<IdLongEntity> Longs => Set<IdLongEntity>();
    public DbSet<IdGuidEntity> Guids => Set<IdGuidEntity>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(identity_strategies.SchemaName);

        modelBuilder.Entity<IdConventionEntity>().ToTable("IdConvention");
        modelBuilder.Entity<IdAlwaysEntity>(entity =>
        {
            entity.ToTable("IdAlways");
            entity.Property(e => e.Id).UseIdentityAlwaysColumn();
        });
        modelBuilder.Entity<IdNeverEntity>(entity =>
        {
            entity.ToTable("IdNever");
            entity.Property(e => e.Id).ValueGeneratedNever();
        });
        modelBuilder.Entity<IdLongEntity>().ToTable("IdLong");
        modelBuilder.Entity<IdGuidEntity>().ToTable("IdGuid");
    }
}
