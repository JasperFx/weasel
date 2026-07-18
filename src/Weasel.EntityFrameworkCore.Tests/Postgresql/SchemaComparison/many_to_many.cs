using Microsoft.EntityFrameworkCore;
using Shouldly;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     Implicit many-to-many join table conventions: the PostTag table with
///     composite PK (PostsId, TagsId), cascading FKs to both sides, and EF's
///     conventional index on the second FK column only (the first is covered
///     by the PK prefix).
/// </summary>
public class many_to_many
{
    public const string SchemaName = "efcmp_m2m";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new ManyToManyDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();

        var joinTable = result.WeaselSchema.TableFor("M2mPostM2mTag");
        joinTable.ShouldNotBeNull();
        joinTable.PrimaryKeyColumns.ShouldBe(["PostsId", "TagsId"]);
        joinTable.ForeignKeys.Count.ShouldBe(2);
        joinTable.ForeignKeys.ShouldAllBe(fk => fk.OnDelete == "CASCADE");

        // Only the second FK column gets a conventional index (PostsId is PK-prefix covered)
        joinTable.IndexFor("IX_M2mPostM2mTag_TagsId").ShouldNotBeNull();
        joinTable.Indexes.Where(i => !i.IsPrimaryKey).Count().ShouldBe(1);
    }
}

public class M2mPost
{
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public List<M2mTag> Tags { get; set; } = [];
}

public class M2mTag
{
    public int Id { get; set; }
    public string Label { get; set; } = string.Empty;
    public List<M2mPost> Posts { get; set; } = [];
}

public class ManyToManyDbContext : DbContext
{
    public DbSet<M2mPost> Posts => Set<M2mPost>();
    public DbSet<M2mTag> Tags => Set<M2mTag>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.HasDefaultSchema(many_to_many.SchemaName);
}
