using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

/// <summary>
/// Tests that EF Core entities with OwnsOne().ToJson() mappings
/// produce a JSON column in the Weasel table definition.
/// See https://github.com/JasperFx/weasel/issues/232
/// </summary>
public class json_column_end_to_end : IAsyncLifetime
{
    private IHost _host = null!;

    public async Task InitializeAsync()
    {
        _host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbContext<JsonColumnDbContext>(options =>
                    options.UseNpgsql(JsonColumnDbContext.ConnectionString));

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
    public void should_map_json_column_from_owned_entity_with_to_json()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<JsonColumnDbContext>();
        var migrator = scope.ServiceProvider.GetRequiredService<Migrator>();

        var entityType = context.Model.FindEntityType(typeof(EntityWithJsonColumn));
        entityType.ShouldNotBeNull();

        var table = migrator.MapToTable(entityType);

        table.ShouldNotBeNull();
        table.Identifier.Name.ShouldBe("entities");

        // Scalar columns should be mapped
        table.HasColumn("id").ShouldBeTrue();
        table.HasColumn("internal_name").ShouldBeTrue();
        table.HasColumn("name").ShouldBeTrue();
        table.HasColumn("description").ShouldBeTrue();

        // The JSON column from OwnsOne().ToJson() should be mapped
        table.HasColumn("extended_properties").ShouldBeTrue("The JSON column 'extended_properties' should be mapped from OwnsOne().ToJson()");
    }

    [Fact]
    public void json_column_should_be_jsonb_type()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<JsonColumnDbContext>();
        var migrator = scope.ServiceProvider.GetRequiredService<Migrator>();

        var entityType = context.Model.FindEntityType(typeof(EntityWithJsonColumn));
        entityType.ShouldNotBeNull();

        var table = migrator.MapToTable(entityType);

        // The JSON column should exist and have the correct type
        table.HasColumn("extended_properties").ShouldBeTrue();
        var pgTable = table.ShouldBeOfType<Weasel.Postgresql.Tables.Table>();
        var column = pgTable.ColumnFor("extended_properties");
        column.ShouldNotBeNull();
        column.Type.ShouldBe("jsonb");
    }

    [Fact]
    public async Task can_apply_migration_with_json_column()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<JsonColumnDbContext>();

        // Clean up any previous test schema
        await context.Database.ExecuteSqlRawAsync("DROP SCHEMA IF EXISTS ef_json_test CASCADE");

        // Use Weasel to create migration and apply it
        var migration = await _host.Services.CreateMigrationAsync(context, CancellationToken.None);
        migration.ShouldNotBeNull();

        await migration.ExecuteAsync(JasperFx.AutoCreate.CreateOrUpdate, CancellationToken.None);

        // Verify data round-trip with JSON column
        var entity = new EntityWithJsonColumn
        {
            Id = Guid.NewGuid(),
            InternalName = "test-entity",
            Name = "Test Entity",
            Description = "A test entity with JSON properties",
            ExtendedProperties = new ExtendedProperties
            {
                Theme = "dark",
                Language = "en",
                MaxItems = 50
            }
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();

        var retrieved = await context.Entities.FindAsync(entity.Id);
        retrieved.ShouldNotBeNull();
        retrieved.ExtendedProperties.Theme.ShouldBe("dark");
        retrieved.ExtendedProperties.Language.ShouldBe("en");
        retrieved.ExtendedProperties.MaxItems.ShouldBe(50);

        // Clean up
        await context.Database.ExecuteSqlRawAsync("DROP SCHEMA IF EXISTS ef_json_test CASCADE");
    }
}
