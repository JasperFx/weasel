# Migrations

Weasel's EF Core integration provides methods to detect schema differences and apply migrations using the same delta-detection engine that powers Weasel's core migration infrastructure.

## Creating a Migration

The `CreateMigrationAsync()` extension method on `IServiceProvider` compares the EF Core model against the actual database state and returns a `DbContextMigration` that can be inspected or applied.

<!-- snippet: sample_efcore_create_migration -->
<a id='snippet-sample_efcore_create_migration'></a>
```cs
// Detect schema changes
await using var migration = await serviceProvider.CreateMigrationAsync(dbContext, ct);

// Check if anything changed
if (migration.Migration.Difference != SchemaPatchDifference.None)
{
    // Apply the migration
    await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, ct);
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreSamples.cs#L72-L82' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_create_migration' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The `DbContextMigration` record wraps three components:

- **`Connection`** -- A `DbConnection` with full credentials for executing DDL.
- **`Migrator`** -- The database-specific `Migrator` (e.g., `PostgresqlMigrator`).
- **`Migration`** -- A `SchemaMigration` containing the detected deltas and DDL patches.

`DbContextMigration` implements `IAsyncDisposable` and disposes the connection when done.

## Creating an IDatabase

For deeper integration with Weasel's `IDatabase` infrastructure (including the CLI tools), use `CreateDatabase()`:

<!-- snippet: sample_efcore_create_database -->
<a id='snippet-sample_efcore_create_database'></a>
```cs
// Create an IDatabaseWithTables for use with Weasel's migration infrastructure
var database = serviceProvider.CreateDatabase(dbContext);

// The database identifier defaults to the DbContext's full type name
// You can also provide a custom identifier:
var customDatabase = serviceProvider.CreateDatabase(dbContext, "my-read-models");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreSamples.cs#L87-L94' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_create_database' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This is useful when composing multiple schema sources (e.g., Marten documents plus EF Core tables) into a single migration pipeline.

## Finding the Right Migrator

`FindMigratorForDbContext()` resolves the correct `Migrator` from the DI container by matching it against the `DbContext`'s connection type:

<!-- snippet: sample_efcore_find_migrator -->
<a id='snippet-sample_efcore_find_migrator'></a>
```cs
// Register migrators in DI
var services = new ServiceCollection();
services.AddSingleton<Migrator>(new PostgresqlMigrator());
// or: services.AddSingleton<Migrator>(new SqlServerMigrator());

// Later, resolve automatically
var (connection, migrator) = serviceProvider.FindMigratorForDbContext(dbContext);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreSamples.cs#L99-L107' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_find_migrator' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

If no registered `Migrator` matches the connection type, an `InvalidOperationException` is thrown listing the available migrators.

## Connection Handling

EF Core sometimes strips credentials from connection strings (e.g., after `EnsureCreatedAsync()` or when using `DbDataSource`). The integration handles this with two internal helpers:

- **`FindDataSource()`** -- Extracts the `DbDataSource` from EF Core's options extensions. When available, connections created from the data source retain full credentials.
- **`GetConnectionWithCredentials()`** -- Falls back through the `DbContext`'s connection string and data source to find a connection with credentials intact.

The `CreateDatabase()` method prefers the data source path when available, which is particularly important for PostgreSQL with `NpgsqlDataSource` where connection pooling and authentication are managed by the data source.

## AutoCreate Options

The `ExecuteAsync()` method accepts an `AutoCreate` enum that controls migration behavior:

| Value | Behavior |
|---|---|
| `AutoCreate.None` | No action taken |
| `AutoCreate.CreateOnly` | Create new objects only, skip updates |
| `AutoCreate.CreateOrUpdate` | Create new objects and update existing ones |
| `AutoCreate.All` | Full migration including destructive changes |
