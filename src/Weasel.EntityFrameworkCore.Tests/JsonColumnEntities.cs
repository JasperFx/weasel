namespace Weasel.EntityFrameworkCore.Tests;

public class ZEntityWithJsonColumn
{
    public Guid Id { get; set; }
    public string InternalName { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
    public AExtendedProperties AExtendedProperties { get; set; } = new();
}

public class AExtendedProperties
{
    public string? Theme { get; set; }
    public string? Language { get; set; }
    public int MaxItems { get; set; }
}
