using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Weasel.Core;
using Weasel.EntityFrameworkCore;

namespace DocSamples;

#region sample_efcore_initial_data
public class TestOrderSeedData : IInitialData<ShopDbContext>
{
    public async Task Populate(ShopDbContext context, CancellationToken cancellation)
    {
        context.Customers.Add(new ShopCustomer { Name = "Test Customer" });
        context.Orders.Add(new ShopOrder { CustomerId = 1, Status = "Pending" });
        await context.SaveChangesAsync(cancellation);
    }
}
#endregion

public class ShopCustomer
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class ShopOrder
{
    public int Id { get; set; }
    public int CustomerId { get; set; }
    public string Status { get; set; } = string.Empty;
}

public class ShopDbContext : DbContext
{
    public ShopDbContext(DbContextOptions<ShopDbContext> options) : base(options) { }
    public DbSet<ShopCustomer> Customers => Set<ShopCustomer>();
    public DbSet<ShopOrder> Orders => Set<ShopOrder>();
}

public class DatabaseCleanerSamples
{
    public void register_database_cleaner()
    {
        #region sample_efcore_register_cleaner
        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddDbContext<ShopDbContext>(options =>
                options.UseNpgsql("Host=localhost;Database=mydb"));

            // Register the Weasel migrator for your database provider
            services.AddSingleton<Migrator, Weasel.Postgresql.PostgresqlMigrator>();

            // Register the database cleaner
            services.AddDatabaseCleaner<ShopDbContext>();

            // Register seed data (optional, runs after ResetAllDataAsync)
            services.AddInitialData<ShopDbContext, TestOrderSeedData>();
        });
        #endregion
    }

    public async Task use_delete_all_data(IHost host)
    {
        #region sample_efcore_delete_all_data
        var cleaner = host.Services.GetRequiredService<IDatabaseCleaner<ShopDbContext>>();

        // Delete all data from tables managed by the DbContext
        // Tables are truncated in FK-safe order (children first)
        await cleaner.DeleteAllDataAsync();
        #endregion
    }

    public async Task use_reset_all_data(IHost host)
    {
        #region sample_efcore_reset_all_data
        var cleaner = host.Services.GetRequiredService<IDatabaseCleaner<ShopDbContext>>();

        // Delete all data, then run all registered IInitialData<T> seeders
        await cleaner.ResetAllDataAsync();
        #endregion
    }

    public async Task use_with_explicit_connection(IHost host)
    {
        #region sample_efcore_multi_tenant_reset
        var cleaner = host.Services.GetRequiredService<IDatabaseCleaner<ShopDbContext>>();

        // For multi-tenant scenarios, pass an explicit connection
        // to target a specific tenant database
        await using var tenantConnection = new Npgsql.NpgsqlConnection(
            "Host=localhost;Database=tenant_abc");

        await cleaner.DeleteAllDataAsync(tenantConnection);
        // or with seeding:
        await cleaner.ResetAllDataAsync(tenantConnection);
        #endregion
    }

    public void register_lambda_initial_data()
    {
        #region sample_efcore_lambda_initial_data
        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddDbContext<ShopDbContext>(options =>
                options.UseNpgsql("Host=localhost;Database=mydb"));

            services.AddSingleton<Migrator, Weasel.Postgresql.PostgresqlMigrator>();
            services.AddDatabaseCleaner<ShopDbContext>();

            // Class-based seeder (as before)
            services.AddInitialData<ShopDbContext, TestOrderSeedData>();

            // Inline lambda seeder — registered as a singleton LambdaInitialData<T>.
            // Runs alongside class-based seeders, in registration order, each time
            // ResetAllDataAsync is invoked.
            services.AddInitialData<ShopDbContext>(async (ctx, ct) =>
            {
                ctx.Customers.Add(new ShopCustomer { Name = "Inline Customer" });
                await ctx.SaveChangesAsync(ct);
            });
        });
        #endregion
    }
}
