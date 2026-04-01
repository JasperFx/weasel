using JasperFx;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Weasel.Core;
using Weasel.EntityFrameworkCore;
using Weasel.Postgresql;

namespace DocSamples;

// === json-columns.md entity types ===

public class Order
{
    public int Id { get; set; }
    public string Status { get; set; } = string.Empty;
    public Address ShippingAddress { get; set; } = null!;
}

public class Address
{
    public string Street { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string PostalCode { get; set; } = string.Empty;
}

public class EfCoreSamples
{
    // Placeholders for DI and DbContext -- these would be provided by application code
    private IServiceProvider serviceProvider = null!;
    private DbContext dbContext = null!;
    private CancellationToken ct;

    // === index.md samples ===

    public async Task efcore_quick_example()
    {
        #region sample_efcore_quick_example
        // Register a Migrator in DI (e.g., PostgresqlMigrator or SqlServerMigrator)
        var services = new ServiceCollection();
        services.AddSingleton<Migrator>(new PostgresqlMigrator());

        // Later, create a migration from a DbContext
        await using var migration = await serviceProvider.CreateMigrationAsync(dbContext, ct);

        if (migration.Migration.Difference != SchemaPatchDifference.None)
        {
            await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, ct);
        }
        #endregion
    }

    // === table-mapping.md samples ===

    public void efcore_table_mapping_basic()
    {
        #region sample_efcore_table_mapping_basic
        var migrator = new PostgresqlMigrator(); // or SqlServerMigrator
        using var context = dbContext;

        foreach (var entityType in DbContextExtensions.GetEntityTypesForMigration(context))
        {
            var table = migrator.MapToTable(entityType);
            // table is now a Weasel ITable with full schema definition
        }
        #endregion
    }

    // === migrations.md samples ===

    public async Task efcore_create_migration()
    {
        #region sample_efcore_create_migration
        // Detect schema changes
        await using var migration = await serviceProvider.CreateMigrationAsync(dbContext, ct);

        // Check if anything changed
        if (migration.Migration.Difference != SchemaPatchDifference.None)
        {
            // Apply the migration
            await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, ct);
        }
        #endregion
    }

    public void efcore_create_database()
    {
        #region sample_efcore_create_database
        // Create an IDatabaseWithTables for use with Weasel's migration infrastructure
        var database = serviceProvider.CreateDatabase(dbContext);

        // The database identifier defaults to the DbContext's full type name
        // You can also provide a custom identifier:
        var customDatabase = serviceProvider.CreateDatabase(dbContext, "my-read-models");
        #endregion
    }

    public void efcore_find_migrator()
    {
        #region sample_efcore_find_migrator
        // Register migrators in DI
        var services = new ServiceCollection();
        services.AddSingleton<Migrator>(new PostgresqlMigrator());
        // or: services.AddSingleton<Migrator>(new SqlServerMigrator());

        // Later, resolve automatically
        var (connection, migrator) = serviceProvider.FindMigratorForDbContext(dbContext);
        #endregion
    }

    // === json-columns.md samples ===

    public async Task efcore_json_migration_example()
    {
        #region sample_efcore_json_migration_example
        await using var migration = await serviceProvider.CreateMigrationAsync(dbContext, ct);

        // The migration will include the JSON column in the table definition.
        // If the column was previously missing (pre-8.11.1), the delta detection
        // will flag it as a new column to add.
        if (migration.Migration.Difference != SchemaPatchDifference.None)
        {
            await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, ct);
        }
        #endregion
    }
}

#region sample_efcore_json_column_configuration
public class OrderDbContext : DbContext
{
    public OrderDbContext(DbContextOptions<OrderDbContext> options) : base(options)
    {
    }

    public DbSet<Order> Orders { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("orders", "myschema");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Status).HasColumnName("status");
            entity.OwnsOne(x => x.ShippingAddress, b =>
            {
                b.ToJson("shipping_address");
            });
        });
    }
}
#endregion
