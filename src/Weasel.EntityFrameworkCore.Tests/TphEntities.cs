namespace Weasel.EntityFrameworkCore.Tests;

// TPH (Table Per Hierarchy) entity hierarchy for testing
// All entities in this hierarchy map to a single "animals" table

public abstract class Animal
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Discriminator { get; set; } = string.Empty;
}

public class Cat : Animal
{
    public bool IsIndoor { get; set; }
}

public class Dog : Animal
{
    public string? FavoriteToy { get; set; }
}

// Separate entity with FK to the TPH base table
public class AnimalOwner
{
    public Guid Id { get; set; }
    public string OwnerName { get; set; } = string.Empty;

    public Guid AnimalId { get; set; }
    public Animal Animal { get; set; } = null!;
}
