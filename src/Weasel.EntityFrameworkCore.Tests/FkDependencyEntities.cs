namespace Weasel.EntityFrameworkCore.Tests;

/// <summary>
/// Test entities with foreign key dependencies for verifying topological sort
/// in GetEntityTypesForMigration. Entity references EntityCategory via FK,
/// so EntityCategory must be created first.
/// See https://github.com/JasperFx/marten/issues/4180
/// </summary>
public class EntityCategory
{
    public int Id { get; set; }
    public string Key { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
}

public class DependentEntity
{
    public Guid Id { get; set; }
    public int CategoryId { get; set; }
    public bool Featured { get; set; }
    public string InternalName { get; set; } = string.Empty;

    public EntityCategory Category { get; set; } = null!;
}
