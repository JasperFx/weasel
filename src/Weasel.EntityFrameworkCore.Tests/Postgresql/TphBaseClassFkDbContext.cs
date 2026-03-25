using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

public class TphBaseClassFkDbContext : DbContext
{
    public TphBaseClassFkDbContext(DbContextOptions<TphBaseClassFkDbContext> options) : base(options)
    {
    }

    public DbSet<Vehicle> Vehicles => Set<Vehicle>();
    public DbSet<Truck> Trucks => Set<Truck>();
    public DbSet<Sedan> Sedans => Set<Sedan>();
    public DbSet<VehicleOwner> VehicleOwners => Set<VehicleOwner>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Vehicle>(entity =>
        {
            entity.ToTable("vehicles");
            entity.HasKey(e => e.Id);
            entity.HasDiscriminator<string>("Discriminator")
                .HasValue<Truck>("Truck")
                .HasValue<Sedan>("Sedan");
        });

        modelBuilder.Entity<VehicleOwner>(entity =>
        {
            entity.ToTable("vehicle_owners");
            entity.HasKey(e => e.Id);
        });
    }
}
