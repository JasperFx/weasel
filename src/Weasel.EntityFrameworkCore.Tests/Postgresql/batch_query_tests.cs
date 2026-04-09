using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Shouldly;
using Weasel.EntityFrameworkCore.Batching;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

public class batch_query_tests : IAsyncLifetime
{
    private IHost _host = null!;

    public async Task InitializeAsync()
    {
        _host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbContext<FkDependencyDbContext>(options =>
                    options.UseNpgsql(FkDependencyDbContext.ConnectionString));
            })
            .Build();

        await _host.StartAsync();

        // Recreate schema and tables
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();
        await context.Database.ExecuteSqlRawAsync(
            $"DROP SCHEMA IF EXISTS {FkDependencyDbContext.TestSchema} CASCADE; CREATE SCHEMA {FkDependencyDbContext.TestSchema};");
        var creator = context.GetService<Microsoft.EntityFrameworkCore.Storage.IRelationalDatabaseCreator>();
        await creator!.CreateTablesAsync();

        // Seed test data
        context.EntityCategories.Add(new EntityCategory { Id = 1, Key = "cat-a", Name = "Category A" });
        context.EntityCategories.Add(new EntityCategory { Id = 2, Key = "cat-b", Name = "Category B" });
        context.EntityCategories.Add(new EntityCategory { Id = 3, Key = "cat-c", Name = "Category C" });
        await context.SaveChangesAsync();

        context.DependentEntities.Add(new DependentEntity
        {
            Id = Guid.NewGuid(), CategoryId = 1, Featured = true, InternalName = "dep-1"
        });
        context.DependentEntities.Add(new DependentEntity
        {
            Id = Guid.NewGuid(), CategoryId = 1, Featured = false, InternalName = "dep-2"
        });
        context.DependentEntities.Add(new DependentEntity
        {
            Id = Guid.NewGuid(), CategoryId = 2, Featured = true, InternalName = "dep-3"
        });
        await context.SaveChangesAsync();
    }

    public async Task DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    [Fact]
    public async Task can_batch_two_list_queries()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();

        await using var batch = context.CreateBatchQuery();

        var categoriesTask = batch.Query(
            context.EntityCategories.Where(c => c.Key.StartsWith("cat-")));

        var dependentsTask = batch.Query(
            context.DependentEntities.Where(d => d.Featured));

        // Single round trip
        await batch.ExecuteAsync();

        var categories = await categoriesTask;
        var dependents = await dependentsTask;

        categories.Count.ShouldBe(3);
        dependents.Count.ShouldBe(2);
        dependents.ShouldAllBe(d => d.Featured);
    }

    [Fact]
    public async Task can_batch_with_ordering()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();

        await using var batch = context.CreateBatchQuery();

        var categoriesTask = batch.Query(
            context.EntityCategories.OrderBy(c => c.Key));

        await batch.ExecuteAsync();

        var categories = await categoriesTask;
        categories.Count.ShouldBe(3);
        categories[0].Key.ShouldBe("cat-a");
        categories[1].Key.ShouldBe("cat-b");
        categories[2].Key.ShouldBe("cat-c");
    }

    [Fact]
    public async Task can_batch_single_entity_query()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();

        await using var batch = context.CreateBatchQuery();

        var categoryTask = batch.QuerySingle(
            context.EntityCategories.Where(c => c.Key == "cat-a"));

        var missingTask = batch.QuerySingle(
            context.EntityCategories.Where(c => c.Key == "nonexistent"));

        await batch.ExecuteAsync();

        var category = await categoryTask;
        var missing = await missingTask;

        category.ShouldNotBeNull();
        category!.Name.ShouldBe("Category A");
        missing.ShouldBeNull();
    }

    [Fact]
    public async Task empty_batch_does_not_throw()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();

        await using var batch = context.CreateBatchQuery();
        await batch.ExecuteAsync(); // Should not throw
    }

    [Fact]
    public async Task can_batch_three_different_entity_queries()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();

        await using var batch = context.CreateBatchQuery();

        var allCats = batch.Query(context.EntityCategories);
        var featuredDeps = batch.Query(
            context.DependentEntities.Where(d => d.Featured));
        var singleCat = batch.QuerySingle(
            context.EntityCategories.Where(c => c.Id == 2));

        await batch.ExecuteAsync();

        (await allCats).Count.ShouldBe(3);
        (await featuredDeps).Count.ShouldBe(2);
        (await singleCat)!.Key.ShouldBe("cat-b");
    }
}
