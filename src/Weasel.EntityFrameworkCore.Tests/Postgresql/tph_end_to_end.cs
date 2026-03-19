using JasperFx;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

public class tph_end_to_end : IAsyncLifetime
{
    private IHost _host = null!;

    public async Task InitializeAsync()
    {
        _host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbContext<TphPostgresqlDbContext>(options =>
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
    public void tph_entities_should_produce_single_table()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TphPostgresqlDbContext>();

        var database = scope.ServiceProvider.CreateDatabase(context, "TphTest");
        database.ShouldNotBeNull();

        // TPH should produce only two tables: "animals" (shared) and "animal_owners"
        // Before the fix, this would produce duplicate table definitions for Cat and Dog
        var tableNames = database.Tables.Select(t => t.Identifier.Name).ToList();
        tableNames.ShouldContain("animals");
        tableNames.ShouldContain("animal_owners");
        tableNames.Count.ShouldBe(2);
    }

    [Fact]
    public void tph_table_should_contain_columns_from_all_subtypes()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TphPostgresqlDbContext>();

        var database = scope.ServiceProvider.CreateDatabase(context, "TphTest2");
        var animalsTable = database.Tables.First(t => t.Identifier.Name == "animals");

        // Should have columns from Animal base class
        animalsTable.HasColumn("id").ShouldBeTrue();
        animalsTable.HasColumn("name").ShouldBeTrue();

        // Should have discriminator column
        animalsTable.HasColumn("discriminator").ShouldBeTrue();

        // Should have columns from Cat subtype
        animalsTable.HasColumn("isindoor").ShouldBeTrue();

        // Should have columns from Dog subtype
        animalsTable.HasColumn("favoritetoy").ShouldBeTrue();
    }

    [Fact]
    public async Task tph_migration_should_not_produce_duplicate_fk_errors()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TphPostgresqlDbContext>();

        // Ensure clean state
        await context.Database.EnsureCreatedAsync();
        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS animal_owners");
        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS animals");

        // This should not throw due to duplicate FK definitions
        var migration = await _host.Services.CreateMigrationAsync(context, CancellationToken.None);
        migration.ShouldNotBeNull();
        migration.Migration.Difference.ShouldNotBe(SchemaPatchDifference.None);

        // Apply the migration - this is where duplicate FK errors would occur
        await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, CancellationToken.None);
    }
}
