using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

public class TphPostgresqlDbContext : DbContext
{
    public TphPostgresqlDbContext(DbContextOptions<TphPostgresqlDbContext> options) : base(options)
    {
    }

    public DbSet<Animal> Animals => Set<Animal>();
    public DbSet<Cat> Cats => Set<Cat>();
    public DbSet<Dog> Dogs => Set<Dog>();
    public DbSet<AnimalOwner> AnimalOwners => Set<AnimalOwner>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Configure TPH: all Animal subclasses in single "animals" table
        modelBuilder.Entity<Animal>(entity =>
        {
            entity.ToTable("animals");
            entity.HasKey(e => e.Id);
            entity.HasDiscriminator<string>("Discriminator")
                .HasValue<Cat>("Cat")
                .HasValue<Dog>("Dog");
        });

        modelBuilder.Entity<AnimalOwner>(entity =>
        {
            entity.ToTable("animal_owners");
            entity.HasKey(e => e.Id);
            entity.HasOne(e => e.Animal)
                .WithMany()
                .HasForeignKey(e => e.AnimalId)
                .OnDelete(DeleteBehavior.Cascade);
        });
    }
}
