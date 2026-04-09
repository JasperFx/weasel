using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Weasel.EntityFrameworkCore.Batching;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.SqlServer;

public class sql_server_batch_query_tests : IAsyncLifetime
{
    private IHost _host = null!;

    public async Task InitializeAsync()
    {
        // Ensure the database exists first
        var masterConn = new SqlConnectionStringBuilder(SqlServerFkDbContext.ConnectionString)
        {
            InitialCatalog = "master"
        };
        await using var conn = new SqlConnection(masterConn.ConnectionString);
        await conn.OpenAsync();
        await using var createDbCmd = conn.CreateCommand();
        createDbCmd.CommandText = """
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'weasel_testing')
            CREATE DATABASE weasel_testing;
        """;
        await createDbCmd.ExecuteNonQueryAsync();

        _host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbContext<SqlServerFkDbContext>(options =>
                    options.UseSqlServer(SqlServerFkDbContext.ConnectionString));
            })
            .Build();

        await _host.StartAsync();

        // Recreate schema and tables
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<SqlServerFkDbContext>();

        await context.Database.ExecuteSqlRawAsync($"""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{SqlServerFkDbContext.TestSchema}')
            EXEC('CREATE SCHEMA {SqlServerFkDbContext.TestSchema}');
        """);

        // Drop existing tables if they exist (child first)
        await context.Database.ExecuteSqlRawAsync($"""
            IF OBJECT_ID('{SqlServerFkDbContext.TestSchema}.products', 'U') IS NOT NULL
                DROP TABLE [{SqlServerFkDbContext.TestSchema}].[products];
            IF OBJECT_ID('{SqlServerFkDbContext.TestSchema}.product_categories', 'U') IS NOT NULL
                DROP TABLE [{SqlServerFkDbContext.TestSchema}].[product_categories];
        """);

        var creator = context.GetService<Microsoft.EntityFrameworkCore.Storage.IRelationalDatabaseCreator>();
        await creator!.CreateTablesAsync();

        // Seed test data
        context.ProductCategories.Add(new ProductCategory { Id = 1, Code = "ELEC", Name = "Electronics" });
        context.ProductCategories.Add(new ProductCategory { Id = 2, Code = "BOOK", Name = "Books" });
        context.ProductCategories.Add(new ProductCategory { Id = 3, Code = "FOOD", Name = "Food" });
        await context.SaveChangesAsync();

        context.Products.Add(new Product { Id = 1, CategoryId = 1, Name = "Laptop", Price = 999.99m, IsActive = true });
        context.Products.Add(new Product { Id = 2, CategoryId = 1, Name = "Phone", Price = 599.99m, IsActive = true });
        context.Products.Add(new Product { Id = 3, CategoryId = 2, Name = "Novel", Price = 14.99m, IsActive = false });
        context.Products.Add(new Product { Id = 4, CategoryId = 3, Name = "Coffee", Price = 9.99m, IsActive = true });
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
        var context = scope.ServiceProvider.GetRequiredService<SqlServerFkDbContext>();

        await using var batch = context.CreateBatchQuery();

        var categoriesTask = batch.Query(context.ProductCategories);
        var activeProductsTask = batch.Query(
            context.Products.Where(p => p.IsActive));

        await batch.ExecuteAsync();

        var categories = await categoriesTask;
        var activeProducts = await activeProductsTask;

        categories.Count.ShouldBe(3);
        activeProducts.Count.ShouldBe(3);
        activeProducts.ShouldAllBe(p => p.IsActive);
    }

    [Fact]
    public async Task can_batch_with_ordering_and_filtering()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<SqlServerFkDbContext>();

        await using var batch = context.CreateBatchQuery();

        var categoriesTask = batch.Query(
            context.ProductCategories.OrderBy(c => c.Code));
        var electronicsTask = batch.Query(
            context.Products.Where(p => p.CategoryId == 1).OrderByDescending(p => p.Price));

        await batch.ExecuteAsync();

        var categories = await categoriesTask;
        categories.Count.ShouldBe(3);
        categories[0].Code.ShouldBe("BOOK");
        categories[1].Code.ShouldBe("ELEC");
        categories[2].Code.ShouldBe("FOOD");

        var electronics = await electronicsTask;
        electronics.Count.ShouldBe(2);
        electronics[0].Name.ShouldBe("Laptop");
        electronics[1].Name.ShouldBe("Phone");
    }

    [Fact]
    public async Task can_batch_single_entity_queries()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<SqlServerFkDbContext>();

        await using var batch = context.CreateBatchQuery();

        var laptopTask = batch.QuerySingle(
            context.Products.Where(p => p.Name == "Laptop"));
        var missingTask = batch.QuerySingle(
            context.Products.Where(p => p.Name == "Nonexistent"));

        await batch.ExecuteAsync();

        var laptop = await laptopTask;
        var missing = await missingTask;

        laptop.ShouldNotBeNull();
        laptop!.Price.ShouldBe(999.99m);
        missing.ShouldBeNull();
    }

    [Fact]
    public async Task can_batch_three_mixed_queries()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<SqlServerFkDbContext>();

        await using var batch = context.CreateBatchQuery();

        var allCategories = batch.Query(context.ProductCategories);
        var cheapProducts = batch.Query(
            context.Products.Where(p => p.Price < 15m));
        var specificCategory = batch.QuerySingle(
            context.ProductCategories.Where(c => c.Id == 2));

        await batch.ExecuteAsync();

        (await allCategories).Count.ShouldBe(3);
        (await cheapProducts).Count.ShouldBe(2); // Novel + Coffee
        (await specificCategory)!.Code.ShouldBe("BOOK");
    }

    [Fact]
    public async Task empty_batch_does_not_throw()
    {
        using var scope = _host.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<SqlServerFkDbContext>();

        await using var batch = context.CreateBatchQuery();
        await batch.ExecuteAsync();
    }
}
