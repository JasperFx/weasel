# EF Core Integration Overview

The `Weasel.EntityFrameworkCore` NuGet package bridges Entity Framework Core's `DbContext` model to Weasel's schema management infrastructure. This allows you to use Weasel's migration tooling, delta detection, and CLI commands with schemas defined through EF Core's fluent API.

## Installation

```bash
dotnet add package Weasel.EntityFrameworkCore
```

The package depends on `Weasel.Core` and `Microsoft.EntityFrameworkCore.Relational`. You still need a database-specific Weasel package (e.g., `Weasel.Postgresql` or `Weasel.SqlServer`) for the `Migrator` implementation.

## Use Cases

**Marten + EF Core side by side** -- When your application uses [Marten](https://martendb.io) for document storage and EF Core for relational entities, both systems can share Weasel's schema management. Marten already uses Weasel internally, so adding the EF Core integration unifies all schema migrations under one tool.

**Polecat + EF Core** -- In event sourcing architectures using Polecat, you can define read model projections with EF Core while Weasel manages all schema migrations across both the event store and the read models.

**Standalone with Weasel CLI** -- Use Weasel's command-line tools to generate migration scripts, assert schema validity, or apply changes for schemas defined entirely through EF Core's `DbContext` configuration.

## Key Types

The integration is centered on a single static class and one record type:

- **`DbContextExtensions`** -- Static extension methods that convert EF Core metadata into Weasel schema objects. This is the primary API surface.
- **`DbContextMigration`** -- A record wrapping a `DbConnection`, `Migrator`, and `SchemaMigration` that can detect and apply schema changes.

## Quick Example

<!-- snippet: sample_efcore_quick_example -->
<a id='snippet-sample_efcore_quick_example'></a>
```cs
// Register a Migrator in DI (e.g., PostgresqlMigrator or SqlServerMigrator)
var services = new ServiceCollection();
services.AddSingleton<Migrator>(new PostgresqlMigrator());

// Later, create a migration from a DbContext
await using var migration = await serviceProvider.CreateMigrationAsync(dbContext, ct);

if (migration.Migration.Difference != SchemaPatchDifference.None)
{
    await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, ct);
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreSamples.cs#L37-L49' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_quick_example' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## How It Works

1. EF Core's `IModel` metadata is read from the `DbContext` (using the design-time model for full annotation access).
2. Each `IEntityType` is converted to a Weasel `ITable` via `Migrator.MapToTable()`, preserving columns, primary keys, foreign keys, indexes, and JSON column mappings.
3. Entity types are topologically sorted by foreign key dependencies so referenced tables are created first.
4. The resulting tables feed into Weasel's standard `SchemaMigration` pipeline for delta detection and DDL generation.
