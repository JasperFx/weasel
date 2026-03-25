using JasperFx;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

/// <summary>
/// Reproduces GitHub issue #228: TPH topological sort fails when the base class
/// has a FK dependency, because derived types inherit the same FK causing duplicate
/// edges that inflate in-degree counts in Kahn's algorithm.
/// </summary>
public class tph_base_class_fk_end_to_end : IAsyncLifetime
{
    private IHost _host = null!;

    public async Task InitializeAsync()
    {
        _host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbContext<TphBaseClassFkDbContext>(options =>
                    options.UseNpgsql(PostgresqlDbContext.ConnectionString));

                services.AddSingleton<Migrator, PostgresqlMigrator>();
            })
            .Build();

        await _host.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    [Fact]
    public void entity_types_sorted_correctly_when_base_class_has_fk()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TphBaseClassFkDbContext>();

        var entityTypes = DbContextExtensions.GetEntityTypesForMigration(context);
        var names = entityTypes.Select(e => e.GetTableName()).ToList();

        // vehicle_owners must come before vehicles because vehicles has a FK to vehicle_owners
        var ownerIndex = names.IndexOf("vehicle_owners");
        var vehicleIndex = names.IndexOf("vehicles");

        ownerIndex.ShouldBeGreaterThanOrEqualTo(0, "vehicle_owners should be in the list");
        vehicleIndex.ShouldBeGreaterThanOrEqualTo(0, "vehicles should be in the list");
        ownerIndex.ShouldBeLessThan(vehicleIndex,
            "vehicle_owners should come before vehicles due to FK dependency");
    }

    [Fact]
    public async Task can_apply_migration_when_base_class_has_fk()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TphBaseClassFkDbContext>();

        // Clean up any previous test state
        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS vehicles");
        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS vehicle_owners");

        var migration = await _host.Services.CreateMigrationAsync(context, CancellationToken.None);
        migration.ShouldNotBeNull();
        migration.Migration.Difference.ShouldNotBe(SchemaPatchDifference.None);

        // This would fail before the fix because the topological sort returned
        // unsorted order, potentially creating vehicles before vehicle_owners
        await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, CancellationToken.None);

        // Clean up
        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS vehicles");
        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS vehicle_owners");
    }
}
