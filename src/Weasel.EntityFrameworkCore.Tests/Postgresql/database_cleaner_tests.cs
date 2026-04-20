using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

[Collection("FkDependencyDbContext")]
public class database_cleaner_tests : IAsyncLifetime
{
    private IHost _host = null!;

    public async Task InitializeAsync()
    {
        _host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbContext<FkDependencyDbContext>(options =>
                    options.UseNpgsql(FkDependencyDbContext.ConnectionString));

                services.AddSingleton<Migrator, PostgresqlMigrator>();
                services.AddDatabaseCleaner<FkDependencyDbContext>();
                services.AddInitialData<FkDependencyDbContext, TestCategorySeedData>();
            })
            .Build();

        await _host.StartAsync();

        // Ensure schema and tables exist using EF Core's own DDL (drop first to avoid stale columns)
        using var setupScope = _host.Services.CreateScope();
        var setupContext = setupScope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();
        await setupContext.Database.ExecuteSqlRawAsync(
            $"DROP SCHEMA IF EXISTS {FkDependencyDbContext.TestSchema} CASCADE; CREATE SCHEMA {FkDependencyDbContext.TestSchema};");
        var creator = setupContext.GetService<Microsoft.EntityFrameworkCore.Storage.IRelationalDatabaseCreator>();
        await creator!.CreateTablesAsync();
    }

    public async Task DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    [Fact]
    public async Task delete_all_data_removes_all_rows()
    {
        // Arrange: insert data respecting FK order
        using (var scope = _host.Services.CreateScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();
            context.EntityCategories.Add(new EntityCategory { Id = 1, Key = "cat1", Name = "Category 1" });
            await context.SaveChangesAsync();

            context.DependentEntities.Add(new DependentEntity
            {
                Id = Guid.NewGuid(), CategoryId = 1, Featured = true, InternalName = "dep1"
            });
            await context.SaveChangesAsync();
        }

        // Act
        var cleaner = _host.Services.GetRequiredService<IDatabaseCleaner<FkDependencyDbContext>>();
        await cleaner.DeleteAllDataAsync();

        // Assert
        using (var scope = _host.Services.CreateScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();
            (await context.EntityCategories.CountAsync()).ShouldBe(0);
            (await context.DependentEntities.CountAsync()).ShouldBe(0);
        }
    }

    [Fact]
    public async Task reset_all_data_deletes_then_seeds()
    {
        // Arrange: insert some data that should be removed
        using (var scope = _host.Services.CreateScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();
            context.EntityCategories.Add(new EntityCategory { Id = 99, Key = "old", Name = "Old Data" });
            await context.SaveChangesAsync();
        }

        // Act
        var cleaner = _host.Services.GetRequiredService<IDatabaseCleaner<FkDependencyDbContext>>();
        await cleaner.ResetAllDataAsync();

        // Assert: old data gone, seed data present
        using (var scope = _host.Services.CreateScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();
            var categories = await context.EntityCategories.ToListAsync();
            categories.ShouldNotContain(c => c.Key == "old");
            categories.ShouldContain(c => c.Key == "seed1");
            categories.ShouldContain(c => c.Key == "seed2");
        }
    }

    [Fact]
    public async Task delete_all_data_on_empty_database_does_not_throw()
    {
        var cleaner = _host.Services.GetRequiredService<IDatabaseCleaner<FkDependencyDbContext>>();

        // Should not throw even if tables are already empty
        await cleaner.DeleteAllDataAsync();
        await cleaner.DeleteAllDataAsync();
    }

    [Fact]
    public async Task delete_all_data_with_explicit_connection_for_multi_tenancy()
    {
        // Arrange
        using (var scope = _host.Services.CreateScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();
            context.EntityCategories.Add(new EntityCategory { Id = 1, Key = "mt", Name = "Multi-tenant" });
            await context.SaveChangesAsync();
        }

        // Act: use explicit connection (simulating multi-tenant scenario)
        var cleaner = _host.Services.GetRequiredService<IDatabaseCleaner<FkDependencyDbContext>>();
        await using var conn = new NpgsqlConnection(FkDependencyDbContext.ConnectionString);
        await cleaner.DeleteAllDataAsync(conn);

        // Assert
        using (var scope = _host.Services.CreateScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();
            (await context.EntityCategories.CountAsync()).ShouldBe(0);
        }
    }
}

public class TestCategorySeedData : IInitialData<FkDependencyDbContext>
{
    public async Task Populate(FkDependencyDbContext context, CancellationToken cancellation)
    {
        context.EntityCategories.Add(new EntityCategory { Id = 1, Key = "seed1", Name = "Seed Category 1" });
        context.EntityCategories.Add(new EntityCategory { Id = 2, Key = "seed2", Name = "Seed Category 2" });
        await context.SaveChangesAsync(cancellation);
    }
}
