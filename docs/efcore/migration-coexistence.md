# Mixed EF Core + Critter Stack Applications

The [migration generation](/efcore/migration-generation) feature is built for applications that combine an EF Core application model with Marten, Wolverine, or Polecat storage in **one database**. This page covers how the pieces coexist.

## Two migration streams, one database

A mixed application has two independent migration streams:

- **Your application's own `DbContext`** with its entities, its migrations, and its `__EFMigrationsHistory` in the default location.
- **The generated Weasel stub context** carrying the critter-stack schema, with its history table **relocated into the critter-stack schema** (e.g. `marten.__EFMigrationsHistory`), so the two never collide.

Both apply cleanly to the same database; each `dotnet ef` invocation targets one stream via `--context`:

```bash
dotnet ef database update --context AppDbContext
dotnet ef database update --context MartenSchemaDbContext
```

At runtime, register both contexts — the stub context needs nothing beyond a connection string and its history table location (the registration snippet is generated into the stub's XML docs):

```csharp
services.AddDbContext<AppDbContext>(o => o.UseNpgsql(connectionString));
services.AddDbContext<MartenSchemaDbContext>(o =>
    o.UseNpgsql(connectionString,
        m => m.MigrationsHistoryTable("__EFMigrationsHistory", "marten")));
```

Multiple `IDatabase` registrations (any mix of Marten + Polecat + Wolverine) produce **per-database migration sets and stub contexts** — select which one to generate for with the same `--database` flag the other `db-*` commands use. For multi-tenancy with a database per tenant, generate **one** migration set and apply the same artifacts against each tenant connection string.

## The single-owner rule

**A table is managed by exactly one migration stream.** The generated Weasel migrations own the critter-stack tables; your application context owns its entity tables. Nothing may own a table twice — otherwise the two streams fight over its shape.

If your application context *maps* critter-stack-managed tables (for querying with EF), exclude them from its migrations:

```csharp
modelBuilder.Entity<OrderProjection>()
    .ToTable("order_projections", "marten",
        t => t.ExcludeFromMigrations());
```

## EF Core projections — the round trip

The full round-trip story for EF-projected documents:

1. Your projection `DbContext` defines the projection tables.
2. [`MapToTable` / `GetSchemaObjectsForMigration`](/efcore/table-mapping) turns that model into Weasel schema objects registered with the `IDatabase`.
3. `db-ef-migration add` then includes the projection tables in the generated migrations automatically — one stream owns them end to end.

Ownership guidance: when a projection table enters the `IDatabase` this way, the **generated Weasel stream owns it** — mark it `ExcludeFromMigrations` in the projection context (which continues to be the query surface). This keeps the single-owner rule intact while both EF (queries) and the critter stack (writes) use the table.

## Verification

The test suite proves coexistence and parity in both directions:

- The dual-schema comparison harness: EF creates a schema → Weasel's delta detection reports `None`, and vice versa.
- The inverted harness: Weasel-defined schemas → generated migrations compiled and applied via the real EF runtime → catalog parity against a Weasel-created schema **and** `SchemaMigration.DetermineAsync` reporting `None`.
- Coexistence scenarios: two generated migration sets (separate schemas and history tables) applying cleanly to one database, and a second application context alongside the stub context.
