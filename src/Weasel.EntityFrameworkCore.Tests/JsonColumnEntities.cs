namespace Weasel.EntityFrameworkCore.Tests;

public class EntityWithJsonColumn
{
    public Guid Id { get; set; }
    public string InternalName { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
    public ExtendedProperties ExtendedProperties { get; set; } = new();
}

public class ExtendedProperties
{
    public string? Theme { get; set; }
    public string? Language { get; set; }
    public int MaxItems { get; set; }
}
