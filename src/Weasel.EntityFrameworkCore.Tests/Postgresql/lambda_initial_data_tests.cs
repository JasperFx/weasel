using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql;
using Xunit;

namespace Weasel.EntityFrameworkCore.Tests.Postgresql;

/// <summary>
/// Tests for <see cref="LambdaInitialData{TContext}" /> and the
/// <c>AddInitialData&lt;TContext&gt;(Func&lt;TContext, CancellationToken, Task&gt;)</c>
/// extension. These sit alongside the class-based
/// <c>AddInitialData&lt;TContext, TData&gt;()</c> form and let callers register small
/// inline seeders without authoring a dedicated class.
/// </summary>
/// <remarks>
/// Placed in the same xUnit collection as <see cref="database_cleaner_tests" /> so the
/// two suites never race on <see cref="FkDependencyDbContext" />'s shared schema.
/// </remarks>
[Collection("FkDependencyDbContext")]
public class lambda_initial_data_tests : IAsyncLifetime
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

                // One class-based seeder + one lambda seeder. The cleaner should run
                // both — in the order they were registered — on ResetAllDataAsync.
                services.AddInitialData<FkDependencyDbContext, TestCategorySeedData>();
                services.AddInitialData<FkDependencyDbContext>(async (ctx, ct) =>
                {
                    ctx.EntityCategories.Add(new EntityCategory
                        { Id = 50, Key = "lambda-seed", Name = "Lambda-registered seed" });
                    await ctx.SaveChangesAsync(ct);
                });
            })
            .Build();

        await _host.StartAsync();

        using var scope = _host.Services.CreateScope();
        var ctx = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();
        await ctx.Database.ExecuteSqlRawAsync(
            $"DROP SCHEMA IF EXISTS {FkDependencyDbContext.TestSchema} CASCADE; CREATE SCHEMA {FkDependencyDbContext.TestSchema};");
        var creator = ctx.GetService<Microsoft.EntityFrameworkCore.Storage.IRelationalDatabaseCreator>();
        await creator!.CreateTablesAsync();
    }

    public async Task DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    [Fact]
    public void lambda_extension_registers_LambdaInitialData_as_an_IInitialData_service()
    {
        // Both the class-based and lambda registrations should be resolvable, and the
        // lambda one specifically should materialize as LambdaInitialData<T>.
        var seeders = _host.Services.GetServices<IInitialData<FkDependencyDbContext>>().ToArray();

        seeders.Length.ShouldBe(2);
        seeders.OfType<LambdaInitialData<FkDependencyDbContext>>().ShouldHaveSingleItem();
        seeders.OfType<TestCategorySeedData>().ShouldHaveSingleItem();
    }

    [Fact]
    public async Task reset_all_data_runs_class_and_lambda_seeders()
    {
        // Stale data that should be cleared before the seeders run.
        using (var scope = _host.Services.CreateScope())
        {
            var ctx = scope.ServiceProvider.GetRequiredService<FkDependencyDbContext>();
            ctx.EntityCategories.Add(new EntityCategory { Id = 99, Key = "stale", Name = "Stale" });
            await ctx.SaveChangesAsync();
        }

        var cleaner = _host.Services.GetRequiredService<IDatabaseCleaner<FkDependencyDbContext>>();
        await cleaner.ResetAllDataAsync();

        using var verify = _host.Services.CreateScope();
        var verifyCtx = verify.ServiceProvider.GetRequiredService<FkDependencyDbContext>();
        var keys = (await verifyCtx.EntityCategories.ToListAsync()).Select(c => c.Key).ToArray();

        keys.ShouldNotContain("stale");
        keys.ShouldContain("seed1");        // class-based TestCategorySeedData
        keys.ShouldContain("seed2");        // class-based TestCategorySeedData
        keys.ShouldContain("lambda-seed");  // LambdaInitialData registration
    }

    [Fact]
    public void add_initial_data_lambda_rejects_null_delegate()
    {
        var services = new ServiceCollection();
        Should.Throw<ArgumentNullException>(
            () => services.AddInitialData<FkDependencyDbContext>(null!));
    }
}
