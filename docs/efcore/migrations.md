# Migrations

Weasel's EF Core integration supports schema migration in **two directions**:

1. **Weasel applies the schema** (this page) — the EF Core model is mapped into Weasel schema objects and Weasel's delta-detection engine detects and applies changes directly, exactly like it does for Marten.
2. **Weasel generates EF Core migration files** — the reverse: Weasel's schema model (including everything an `IDatabase` carries) is emitted as standard, compilable EF Core migrations that your team applies with `dotnet ef database update`, idempotent scripts, or bundles. See [EF Core Migration Generation](/efcore/migration-generation) and the [coexistence guide](/efcore/migration-coexistence).

The rest of this page covers the first direction: detecting schema differences and applying migrations using the same delta-detection engine that powers Weasel's core migration infrastructure.

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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreSamples.cs#L84-L94' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_create_migration' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreSamples.cs#L99-L106' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_create_database' title='Start of snippet'>anchor</a></sup>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreSamples.cs#L111-L119' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_find_migrator' title='Start of snippet'>anchor</a></sup>
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

## Add-Only Migrations for EF-Derived Models

`AutoCreate.CreateOrUpdate` normally drops the columns, indexes and foreign keys the model no longer
declares. For a table defined directly in code that is the point: the model is the whole truth about
the schema.

For a table **translated** from an EF Core model it is not. The mapper reads EF Core's relational
model, and there are shapes it does not translate (see
[What Is Not Translated](./table-mapping#what-is-not-translated)). A column EF Core knows about and
the mapper cannot express is not "removed from the model" -- it is "not understood" -- so reading it
as a removal turns every gap in the translation into a data-loss branch. That is not hypothetical:
before [weasel#629](https://github.com/JasperFx/weasel/issues/629), a table-split `ComplexProperty`
was such a gap, and migrating an EF-built table executed `drop column total_amount`.

So every table from `MapToTable()` sets `ITable.AddOnlyMigrations`:

- a column, index or foreign key the model does not declare is **left in place**
- everything additive still applies -- new columns, new indexes, new foreign keys
- everything in-place still applies -- a changed index is recreated, a widened type is altered
- the withheld drops are logged once per table before a migration runs, and are readable from the
  delta (`ISchemaObjectDeltaWithWithheldDrops.WithheldDrops`) at any time

A migration whose *only* difference is a withheld drop reports `SchemaPatchDifference.None`, so it
is not an update that writes nothing, and it does not log the same warning on every application
start.

### Allowing drops

When the EF model really is the whole truth about these tables -- a removed property should take its
column with it -- set `AllowDrops` on the mapping customization:

<!-- snippet: sample_efcore_allow_drops -->
<a id='snippet-sample_efcore_allow_drops'></a>
```cs
// EF-derived tables are add-only by default: a column, index or foreign key the mapper
// could not translate is left in place rather than dropped (weasel#629). Set AllowDrops
// when the EF model really is the whole truth about these tables.
var customization = new EfSchemaMappingCustomization { AllowDrops = true };

await using var migration =
    await serviceProvider.CreateMigrationAsync(dbContext, customization, ct);
await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, ct);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreCustomizationSamples.cs#L67-L76' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_allow_drops' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

For finer control, clear `ITable.AddOnlyMigrations` on individual tables from `CustomizeTable`, which
runs after the mapping:

<!-- snippet: sample_efcore_allow_drops_per_table -->
<a id='snippet-sample_efcore_allow_drops_per_table'></a>
```cs
var customization = new EfSchemaMappingCustomization
{
    // CustomizeTable runs AFTER the mapping, so it has the last word on the policy
    CustomizeTable = (entityType, table) =>
    {
        if (entityType.ClrType.Name == "StagingRow")
        {
            table.AddOnlyMigrations = false;
        }
    }
};

var database = serviceProvider.CreateDatabase(dbContext, customization);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreCustomizationSamples.cs#L81-L95' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_allow_drops_per_table' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

::: warning
`AddOnlyMigrations` is about *removal*, not about data. It does not make `AutoCreate.All` safe: a
change that can only be applied by dropping and recreating the table still drops it. Use
`Migrator.RefuseDestructiveChanges` for that.
:::
