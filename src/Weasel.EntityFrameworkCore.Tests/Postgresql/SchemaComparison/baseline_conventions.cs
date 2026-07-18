using Microsoft.EntityFrameworkCore;
using Weasel.EntityFrameworkCore.Tests.SchemaComparison;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql.SchemaComparison;

/// <summary>
///     Baseline: a model using nothing but EF Core default conventions —
///     PascalCase table/column names, int identity primary keys, a conventional
///     FK with its conventional IX_ index, required/optional strings. If Weasel
///     can't reproduce this schema, nothing else matters.
/// </summary>
public class baseline_conventions
{
    public const string SchemaName = "efcmp_baseline";

    [Fact]
    public async Task weasel_schema_matches_ef_schema()
    {
        await using var context = new BaselineDbContext();

        var result = await SchemaComparisonHarness.RunPostgresqlAsync(context, SchemaName);

        result.AssertParity();
    }
}

public class BaselineBlog
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}

public class BaselinePost
{
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public int BlogId { get; set; }
    public BaselineBlog Blog { get; set; } = null!;
}

public class BaselineDbContext : DbContext
{
    public DbSet<BaselineBlog> Blogs => Set<BaselineBlog>();
    public DbSet<BaselinePost> Posts => Set<BaselinePost>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseNpgsql(PostgresqlDbContext.ConnectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.HasDefaultSchema(baseline_conventions.SchemaName);
}
