# Batch Queries

Weasel provides a batch query API for EF Core that combines multiple queries into a single database round trip. This is similar to [Marten's IBatchedQuery](https://martendb.io/documents/querying/batched-queries) and addresses a long-standing EF Core feature request ([dotnet/efcore#10879](https://github.com/dotnet/efcore/issues/10879)).

## Why Batch?

Every database query is a network round trip. If a request handler needs three queries, that's three round trips. Batching combines them into one, which can be the single biggest performance improvement for database-heavy endpoints.

## Basic Usage

Create a `BatchedQuery` from your `DbContext`, queue queries that return `Task<T>` futures, then call `ExecuteAsync()`:

<!-- snippet: sample_efcore_batch_query_basic -->
<a id='snippet-sample_efcore_batch_query_basic'></a>
```cs
await using var batch = context.CreateBatchQuery();

// Queue multiple queries — each returns a Task (future)
var customersTask = batch.Query(
    context.Customers.Where(c => c.Name.StartsWith("A")));

var ordersTask = batch.Query(
    context.Orders.Where(o => o.Status == "Pending"));

// Single database round trip for both queries
await batch.ExecuteAsync();

// Results are now resolved
var customers = await customersTask;
var orders = await ordersTask;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/BatchQuerySamples.cs#L10-L26' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_batch_query_basic' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Single Entity Queries

Use `QuerySingle<T>()` for queries expected to return zero or one result:

<!-- snippet: sample_efcore_batch_query_single -->
<a id='snippet-sample_efcore_batch_query_single'></a>
```cs
await using var batch = context.CreateBatchQuery();

// QuerySingle returns a single entity or null
var customerTask = batch.QuerySingle(
    context.Customers.Where(c => c.Id == 42));

var orderTask = batch.QuerySingle(
    context.Orders.Where(o => o.Id == 100));

await batch.ExecuteAsync();

var customer = await customerTask; // may be null
var order = await orderTask;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/BatchQuerySamples.cs#L31-L45' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_batch_query_single' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Mixing Query Types

You can mix list queries and single entity lookups in the same batch:

<!-- snippet: sample_efcore_batch_query_mixed -->
<a id='snippet-sample_efcore_batch_query_mixed'></a>
```cs
await using var batch = context.CreateBatchQuery();

// Mix list queries, single entity lookups, and filtered queries
var allCustomers = batch.Query(context.Customers);
var pendingOrders = batch.Query(
    context.Orders.Where(o => o.Status == "Pending").OrderBy(o => o.Id));
var specificCustomer = batch.QuerySingle(
    context.Customers.Where(c => c.Id == 1));

// All three execute in a single round trip
await batch.ExecuteAsync();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/BatchQuerySamples.cs#L50-L62' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_batch_query_mixed' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## How It Works

1. **SQL extraction**: Each `IQueryable<T>` is compiled to SQL via EF Core's `CreateDbCommand()` — a public, stable API that returns a `DbCommand` with parameterized SQL
2. **Batch assembly**: All commands are packed into a single `DbBatch` (ADO.NET's native batching abstraction, available in .NET 8+)
3. **Single execution**: The batch executes in one database round trip, returning a `DbDataReader` with multiple result sets
4. **Materialization**: Each result set is read sequentially via `NextResultAsync()` and materialized using EF Core's entity type metadata (column names, types, value converters)
5. **Resolution**: Results are pushed through `TaskCompletionSource<T>`, resolving the futures returned to the caller

## Supported Providers

Any ADO.NET provider that supports `DbBatch` works with this API:

| Provider | Support |
|----------|---------|
| PostgreSQL (Npgsql) | Full support |
| SQL Server (Microsoft.Data.SqlClient) | Full support |
| SQLite (Microsoft.Data.Sqlite) | Full support |

## Limitations

- **No change tracking**: Materialized entities are not tracked by EF Core's `ChangeTracker`. Use this for read-only queries.
- **Simple entity types**: The materializer handles flat entities with scalar properties, value converters, and nullable columns. Complex owned types and navigation properties require loading through EF Core's standard query pipeline.
- **IQueryable only**: Queries must be expressible as `IQueryable<T>` — raw SQL queries are not yet supported in the batch API.
