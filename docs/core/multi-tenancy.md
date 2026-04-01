# Multi-Tenancy

Weasel provides foundational building blocks for multi-tenant database strategies through the `Weasel.Core.MultiTenancy` namespace. These abstractions handle the mechanics of distributing tenants across a pool of databases and are primarily consumed by higher-level libraries like [Marten](https://martendb.io) and [Wolverine](https://wolverinefx.io).

## Overview

In a sharded multi-tenant architecture, each tenant's data lives in one of several databases. Weasel provides the low-level plumbing for this pattern:

- **A pool of databases** that can accept tenants
- **Assignment strategies** that decide which database a new tenant goes to
- **Tracking** of which tenants are in which database
- **CLI integration** so migrations run across all tenant databases

## Core Abstractions

### ITenantDatabasePool

Manages the registry of databases and tenant assignments:

<!-- snippet: sample_ITenantDatabasePool_interface -->
<a id='snippet-sample_itenantdatabasepool_interface'></a>
```cs
public interface ITenantDatabasePool_Sample
{
    ValueTask<IReadOnlyList<PooledDatabase>> ListDatabasesAsync(CancellationToken ct);
    ValueTask AddDatabaseAsync(string databaseId, string connectionString, CancellationToken ct);
    ValueTask MarkDatabaseFullAsync(string databaseId, CancellationToken ct);
    ValueTask<string?> FindDatabaseForTenantAsync(string tenantId, CancellationToken ct);
    ValueTask AssignTenantAsync(string tenantId, string databaseId, CancellationToken ct);
    ValueTask RemoveTenantAsync(string tenantId, CancellationToken ct);
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MultiTenancySamples.cs#L7-L17' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_itenantdatabasepool_interface' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Each `PooledDatabase` tracks a database's identifier, connection string, and whether it is full (accepting no more tenants).

### ITenantAssignmentStrategy

Determines which database a new tenant should be assigned to:

<!-- snippet: sample_ITenantAssignmentStrategy_interface -->
<a id='snippet-sample_itenantassignmentstrategy_interface'></a>
```cs
public interface ITenantAssignmentStrategy_Sample
{
    ValueTask<string> AssignTenantToDatabaseAsync(
        string tenantId,
        IReadOnlyList<PooledDatabase> availableDatabases);
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MultiTenancySamples.cs#L19-L26' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_itenantassignmentstrategy_interface' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Weasel ships with several built-in strategies:

| Strategy | Behavior |
|----------|----------|
| `SmallestTenantAssignment` | Assigns to the database with the fewest tenants (balanced distribution). |
| `HashTenantAssignment` | Uses a hash of the tenant ID for deterministic, even distribution. |
| `ExplicitTenantAssignment` | Requires manual mapping of each tenant to a specific database. |

### IDatabaseSizingStrategy

Controls when a database is considered "full" and should stop accepting new tenants:

<!-- snippet: sample_IDatabaseSizingStrategy_interface -->
<a id='snippet-sample_idatabasesizingstrategy_interface'></a>
```cs
public interface IDatabaseSizingStrategy_Sample
{
    ValueTask<string> FindSmallestDatabaseAsync(IReadOnlyList<PooledDatabase> databases);
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MultiTenancySamples.cs#L28-L33' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_idatabasesizingstrategy_interface' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## IDatabase.TenantIds

The `IDatabase` interface includes a `TenantIds` property that lists which tenants are assigned to that database instance:

<!-- snippet: sample_IDatabase_TenantIds -->
<a id='snippet-sample_idatabase_tenantids'></a>
```cs
public interface IDatabase_TenantIds_Sample
{
    List<string> TenantIds { get; }
    // ... other members
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MultiTenancySamples.cs#L37-L43' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_idatabase_tenantids' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This is used by the migration infrastructure to apply schema changes to the correct databases when running in a multi-tenant configuration.

## CLI Integration

Weasel's command-line tools (accessed through the JasperFx CLI) are multi-tenancy aware:

- **`db-apply`** -- applies pending migrations across all tenant databases in the pool.
- **`db-assert`** -- verifies schema consistency across all tenant databases.
- **`--database` flag** -- filters CLI operations to a specific database by identifier, useful for targeting a single tenant database during troubleshooting.

## Typical Usage

Multi-tenancy in Weasel is designed to be consumed through Marten or Wolverine rather than used directly. A typical setup through Marten looks like:

1. Configure a `ITenantDatabasePool` implementation (often backed by a master database table).
2. Choose an `ITenantAssignmentStrategy` for new tenant distribution.
3. Marten handles the rest: resolving connections per tenant, running migrations across all databases, and routing queries to the right database.

If you are building a custom multi-tenant system without Marten, you would implement `ITenantDatabasePool` for your storage mechanism and use `ITenantAssignmentStrategy` to place new tenants. The `IDatabase` infrastructure then ensures schema migrations are applied to every database in the pool.
