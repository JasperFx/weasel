using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     Alternate keys: HasAlternateKey (single and composite) become UNIQUE
///     constraints in EF migrations; Weasel models them as unique indexes.
///     PostgreSQL implements both with a unique index, so the only residual
///     difference is constraint-backed vs plain — a tolerated category.
///     Also covers an FK targeting the alternate key via HasPrincipalKey.
/// </summary>
public class alternate_keys
{
    public const string SchemaName = "efcmp_altkeys";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new AlternateKeyDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        // EF creates AK_* as a UNIQUE CONSTRAINT; Weasel creates a unique INDEX.
        // Functionally equivalent on PostgreSQL (FKs can target either).
        result.AssertParity(DifferenceCategory.UniqueConstraintVsUniqueIndex);

        var efUsers = result.EfSchema.TableFor("AkUsers")!;
        efUsers.IndexFor("AK_AkUsers_Email")!.IsUnique.ShouldBeTrue();
        efUsers.IndexFor("AK_AkUsers_Region_LocalId")!.KeyColumns.ShouldBe(["Region", "LocalId"]);

        var weaselUsers = result.WeaselSchema.TableFor("AkUsers")!;
        weaselUsers.IndexFor("AK_AkUsers_Email")!.IsUnique.ShouldBeTrue();

        // The FK must reference the alternate key's columns
        var fk = result.WeaselSchema.TableFor("AkLogins")!.ForeignKeys.Single();
        fk.PrincipalColumns.ShouldBe(["Email"]);
    }
}

public class AkUser
{
    public int Id { get; set; }
    public string Email { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public int LocalId { get; set; }
}

public class AkLogin
{
    public int Id { get; set; }
    public string UserEmail { get; set; } = string.Empty;
    public AkUser User { get; set; } = null!;
}

public class AlternateKeyDbContext : DbContext
{
    public DbSet<AkUser> Users => Set<AkUser>();
    public DbSet<AkLogin> Logins => Set<AkLogin>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(alternate_keys.SchemaName);

        modelBuilder.Entity<AkUser>(entity =>
        {
            entity.ToTable("AkUsers");
            entity.HasAlternateKey(e => e.Email);
            entity.HasAlternateKey(e => new { e.Region, e.LocalId });
        });

        modelBuilder.Entity<AkLogin>(entity =>
        {
            entity.ToTable("AkLogins");
            entity.HasOne(e => e.User)
                .WithMany()
                .HasForeignKey(e => e.UserEmail)
                .HasPrincipalKey(e => e.Email)
                .OnDelete(DeleteBehavior.Cascade);
        });
    }
}
