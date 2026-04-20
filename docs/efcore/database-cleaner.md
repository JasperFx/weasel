# Database Reset for Testing

Weasel provides an FK-aware database cleaner for EF Core that makes integration testing straightforward. Inspired by [Respawn](https://github.com/jbogard/respawn) and Marten's `ResetAllData()`, the cleaner discovers tables from your DbContext metadata, resolves foreign key ordering, and generates provider-specific SQL to safely truncate all data.

## Setup

Register the cleaner and optional seed data in your DI container:

<!-- snippet: sample_efcore_register_cleaner -->
<a id='snippet-sample_efcore_register_cleaner'></a>
```cs
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
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/DatabaseCleanerSamples.cs#L45-L61' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_register_cleaner' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Deleting All Data

Use `DeleteAllDataAsync()` to truncate all tables in FK-safe order (children before parents):

<!-- snippet: sample_efcore_delete_all_data -->
<a id='snippet-sample_efcore_delete_all_data'></a>
```cs
var cleaner = host.Services.GetRequiredService<IDatabaseCleaner<ShopDbContext>>();

// Delete all data from tables managed by the DbContext
// Tables are truncated in FK-safe order (children first)
await cleaner.DeleteAllDataAsync();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/DatabaseCleanerSamples.cs#L66-L72' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_delete_all_data' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Behind the scenes, the cleaner generates provider-specific SQL:

- **PostgreSQL**: `TRUNCATE TABLE t1, t2, t3 RESTART IDENTITY CASCADE;`
- **SQL Server**: `DELETE FROM` each table in dependency order, then `DBCC CHECKIDENT` to reseed
- **SQLite**: `DELETE FROM` each table in dependency order, then clears `sqlite_sequence`

## Reset with Seed Data

Use `ResetAllDataAsync()` to delete all data and then run all registered `IInitialData<TContext>` seeders:

<!-- snippet: sample_efcore_reset_all_data -->
<a id='snippet-sample_efcore_reset_all_data'></a>
```cs
var cleaner = host.Services.GetRequiredService<IDatabaseCleaner<ShopDbContext>>();

// Delete all data, then run all registered IInitialData<T> seeders
await cleaner.ResetAllDataAsync();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/DatabaseCleanerSamples.cs#L77-L82' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_reset_all_data' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## IInitialData

Implement `IInitialData<TContext>` to define seed data applied after a reset. Multiple seeders execute in registration order.

<!-- snippet: sample_efcore_initial_data -->
<a id='snippet-sample_efcore_initial_data'></a>
```cs
public class TestOrderSeedData : IInitialData<ShopDbContext>
{
    public async Task Populate(ShopDbContext context, CancellationToken cancellation)
    {
        context.Customers.Add(new ShopCustomer { Name = "Test Customer" });
        context.Orders.Add(new ShopOrder { CustomerId = 1, Status = "Pending" });
        await context.SaveChangesAsync(cancellation);
    }
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/DatabaseCleanerSamples.cs#L9-L19' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_initial_data' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

### Inline lambda seeders

For small amounts of seed data, authoring a dedicated `IInitialData<TContext>` class is often overkill. The `AddInitialData<TContext>(Func<TContext, CancellationToken, Task>)` overload wraps a delegate in a `LambdaInitialData<TContext>` and registers it as a singleton `IInitialData<TContext>`. Lambda and class-based seeders coexist and run in registration order:

<!-- snippet: sample_efcore_lambda_initial_data -->
<a id='snippet-sample_efcore_lambda_initial_data'></a>
```cs
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
```
<!-- endSnippet -->

The lambda receives a scoped `TContext` and the caller's `CancellationToken`. Call `SaveChangesAsync` as normal — the cleaner does not wrap the seeder in a transaction.

## Multi-Tenancy

For multi-tenant scenarios where each tenant has its own database, pass an explicit `DbConnection` to target a specific tenant:

<!-- snippet: sample_efcore_multi_tenant_reset -->
<a id='snippet-sample_efcore_multi_tenant_reset'></a>
```cs
var cleaner = host.Services.GetRequiredService<IDatabaseCleaner<ShopDbContext>>();

// For multi-tenant scenarios, pass an explicit connection
// to target a specific tenant database
await using var tenantConnection = new Npgsql.NpgsqlConnection(
    "Host=localhost;Database=tenant_abc");

await cleaner.DeleteAllDataAsync(tenantConnection);
// or with seeding:
await cleaner.ResetAllDataAsync(tenantConnection);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/DatabaseCleanerSamples.cs#L87-L98' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_multi_tenant_reset' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Performance

The cleaner **memoizes** the table dependency graph and generated SQL on first use. The EF Core model is immutable, so the SQL never changes. This means subsequent calls in a test suite reuse the cached SQL with zero overhead — critical when running hundreds of tests.

## Supported Providers

| Provider | Strategy | Identity Reset |
|----------|----------|---------------|
| PostgreSQL | `TRUNCATE ... CASCADE RESTART IDENTITY` | Automatic |
| SQL Server | `DELETE FROM` in FK order | `DBCC CHECKIDENT` |
| SQLite | `DELETE FROM` in FK order | Clears `sqlite_sequence` |
| MySQL | `TRUNCATE TABLE` with `FOREIGN_KEY_CHECKS=0` | Automatic |
| Oracle | `DELETE FROM` in FK order | N/A (uses sequences) |
