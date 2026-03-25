namespace Weasel.EntityFrameworkCore.Tests;

// TPH hierarchy where the base class has a FK to another entity.
// This reproduces GitHub issue #228: the FK on Animal is inherited by
// Cat and Dog, causing duplicate dependency edges in the topological sort
// which breaks Kahn's algorithm (in-degree inflated but only decremented once).

public abstract class Vehicle
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Discriminator { get; set; } = string.Empty;
    public VehicleOwner Owner { get; set; } = default!;
}

public class Truck : Vehicle
{
    public double PayloadCapacity { get; set; }
}

public class Sedan : Vehicle
{
    public int PassengerCount { get; set; }
}

public class VehicleOwner
{
    public Guid Id { get; set; }
    public string OwnerName { get; set; } = string.Empty;
    public List<Vehicle> Vehicles { get; set; } = [];
}
