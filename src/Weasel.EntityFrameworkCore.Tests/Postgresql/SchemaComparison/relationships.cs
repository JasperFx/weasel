using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     Relationship shapes: self-referencing FK, shadow FK columns, and the
///     full DeleteBehavior matrix — especially the Client* behaviors, for which
///     EF emits NO ON DELETE clause (ClientSetNull being the default for
///     optional relationships).
/// </summary>
public class relationships
{
    public const string SchemaName = "efcmp_rels";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new RelationshipsDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();

        var nodes = result.WeaselSchema.TableFor("TreeNodes")!;
        // optional self-reference: ClientSetNull -> no ON DELETE clause
        nodes.ForeignKeys.Single().OnDelete.ShouldBe("NO ACTION");
        nodes.IndexFor("IX_TreeNodes_ParentId").ShouldNotBeNull();

        var actions = result.WeaselSchema.TableFor("RefActions")!;
        actionFor(actions, "CascadeRefId").ShouldBe("CASCADE");
        actionFor(actions, "SetNullRefId").ShouldBe("SET NULL");
        actionFor(actions, "RestrictRefId").ShouldBe("RESTRICT");
        actionFor(actions, "NoActionRefId").ShouldBe("NO ACTION");
        actionFor(actions, "ClientSetNullRefId").ShouldBe("NO ACTION");
        // shadow FK column created by convention from the navigation
        actions.ColumnFor("ShadowRefId").ShouldNotBeNull();
    }

    private static string actionFor(TableSnapshot table, string column)
        => table.ForeignKeys.Single(fk => fk.Columns.SequenceEqual([column])).OnDelete;
}

public class TreeNode
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int? ParentId { get; set; }
    public TreeNode? Parent { get; set; }
    public List<TreeNode> Children { get; set; } = [];
}

public class RefTarget
{
    public int Id { get; set; }
}

public class RefAction
{
    public int Id { get; set; }
    public int CascadeRefId { get; set; }
    public int? SetNullRefId { get; set; }
    public int RestrictRefId { get; set; }
    public int NoActionRefId { get; set; }
    public int? ClientSetNullRefId { get; set; }
    public RefTarget ShadowRef { get; set; } = null!;
}

public class RelationshipsDbContext : DbContext
{
    public DbSet<TreeNode> TreeNodes => Set<TreeNode>();
    public DbSet<RefTarget> RefTargets => Set<RefTarget>();
    public DbSet<RefAction> RefActions => Set<RefAction>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema(relationships.SchemaName);

        modelBuilder.Entity<TreeNode>(entity =>
        {
            entity.ToTable("TreeNodes");
            entity.HasOne(e => e.Parent)
                .WithMany(e => e.Children)
                .HasForeignKey(e => e.ParentId);
        });

        modelBuilder.Entity<RefAction>(entity =>
        {
            entity.ToTable("RefActions");
            entity.HasOne<RefTarget>().WithMany().HasForeignKey(e => e.CascadeRefId)
                .OnDelete(DeleteBehavior.Cascade);
            entity.HasOne<RefTarget>().WithMany().HasForeignKey(e => e.SetNullRefId)
                .OnDelete(DeleteBehavior.SetNull);
            entity.HasOne<RefTarget>().WithMany().HasForeignKey(e => e.RestrictRefId)
                .OnDelete(DeleteBehavior.Restrict);
            entity.HasOne<RefTarget>().WithMany().HasForeignKey(e => e.NoActionRefId)
                .OnDelete(DeleteBehavior.NoAction);
            entity.HasOne<RefTarget>().WithMany().HasForeignKey(e => e.ClientSetNullRefId)
                .OnDelete(DeleteBehavior.ClientSetNull);
            // navigation without an explicit FK property -> shadow column ShadowRefId
            entity.HasOne(e => e.ShadowRef).WithMany().OnDelete(DeleteBehavior.Cascade);
        });
    }
}
