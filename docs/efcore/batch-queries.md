# Batch Queries

Weasel provides a batch query API for EF Core that combines multiple queries into a single database round trip. This is similar to [Marten's IBatchedQuery](https://martendb.io/documents/querying/batched-queries) and addresses a long-standing EF Core feature request ([dotnet/efcore#10879](https://github.com/dotnet/efcore/issues/10879)).

::: tip
If you're using **Wolverine** with EF Core, Wolverine can auto-batch queries inside your message handlers. See [Wolverine's batch query documentation](https://wolverinefx.io/guide/durability/efcore/batch-queries.html) for handler-specific patterns. This page covers the underlying `BatchedQuery` fluent API that Wolverine builds on.
:::

## Why Batch?

Every database query is a network round trip. If a request handler needs three queries, that's three round trips. Batching combines them into one, which can be the single biggest performance improvement for database-heavy endpoints.

In benchmarks on a local SQL Server with 4 keyed lookups per handler invocation, batching delivers a **2.78× speedup** (6.92 ms → 2.49 ms per handler). The improvement scales with the number of queries and with network latency — across a region-to-region hop, a four-query handler can drop from ~40 ms to ~12 ms.

## API Reference

`BatchedQuery` exposes three query methods. Each compiles the `IQueryable<T>` to SQL immediately and returns a `Task<T>` future that is resolved when `ExecuteAsync()` is called.

| Method | Returns | Description |
|--------|---------|-------------|
| `Query<T>(IQueryable<T>)` | `Task<IReadOnlyList<T>>` | Returns all matching rows as a list. |
| `QuerySingle<T>(IQueryable<T>)` | `Task<T?>` | Returns the first matching row, or `null` if none. |
| `Scalar<T>(IQueryable<T>)` | `Task<T>` | Returns a single scalar value (e.g., from a COUNT or MAX projection). |
| `ExecuteAsync(CancellationToken)` | `Task` | Sends all queued queries in one round trip and resolves every future. |

The `DbContext.CreateBatchQuery()` extension method creates a new `BatchedQuery` bound to that context's connection and transaction.

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

## Lifecycle and Disposal

`BatchedQuery` implements `IAsyncDisposable`. The query lifecycle has three phases:

<!-- snippet: sample_efcore_batch_lifecycle -->
<a id='snippet-sample_efcore_batch_lifecycle'></a>
```cs
// BatchedQuery implements IAsyncDisposable. Always use 'await using'
// to ensure the underlying DbCommands are properly disposed.
await using var batch = context.CreateBatchQuery();

// 1. Queue phase — SQL is compiled immediately via CreateDbCommand(),
//    but nothing is sent to the database yet.
var customersTask = batch.Query(context.Customers);
var ordersTask = batch.Query(context.Orders);

// 2. Execute phase — all queued queries are sent as a single DbBatch.
//    Each Task<T> future is resolved as its result set is read.
await batch.ExecuteAsync();

// 3. Consume phase — awaiting the futures is instantaneous because
//    ExecuteAsync already resolved them.
var customers = await customersTask;
var orders = await ordersTask;

// A BatchedQuery is single-use. Do not call ExecuteAsync() again
// or queue additional queries after execution.
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/BatchQuerySamples.cs#L67-L88' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_batch_lifecycle' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A `BatchedQuery` is **single-use**. Do not call `ExecuteAsync()` more than once or queue additional queries after execution. Create a new batch for each unit of work.

## Error Handling

If any query in the batch fails (e.g., a SQL syntax error or connection failure), the entire batch fails. None of the `Task<T>` futures will be resolved — awaiting them after a failed `ExecuteAsync()` will throw.

<!-- snippet: sample_efcore_batch_error_handling -->
<a id='snippet-sample_efcore_batch_error_handling'></a>
```cs
await using var batch = context.CreateBatchQuery();

var customersTask = batch.Query(context.Customers);
var ordersTask = batch.Query(context.Orders);

try
{
    await batch.ExecuteAsync();
}
catch (Exception ex)
{
    // If any query in the batch fails, the entire batch fails.
    // None of the Task<T> futures will be resolved — awaiting
    // them after a failed ExecuteAsync will throw.
    Console.WriteLine($"Batch failed: {ex.Message}");
    return;
}

// Safe to await only after successful ExecuteAsync
var customers = await customersTask;
var orders = await ordersTask;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/BatchQuerySamples.cs#L93-L115' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_batch_error_handling' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Execution Semantics

**Order**: Queries execute in the order they were queued. Result sets are read sequentially via `NextResultAsync()`.

**Independence**: Each query in the batch is independent. Results from one query cannot feed into another within the same batch. If you need dependent queries, execute the first batch, await the result, then build a second batch.

**Thread safety**: `BatchedQuery` is **not thread-safe**. All `Query`/`QuerySingle`/`Scalar` calls and the `ExecuteAsync` call must happen on the same async context (which is the natural pattern in request handlers and test methods).

**Transaction awareness**: If the `DbContext` has an active transaction (`Database.CurrentTransaction`), the batch executes within that transaction.

## Change Tracking

Entities loaded via `BatchedQuery` are **not tracked** by EF Core's `ChangeTracker`. This is a deliberate design choice — batch queries are optimized for read-only scenarios where you need data fast. If you need to modify an entity loaded from a batch, either:

- Attach it to the context with `context.Attach(entity)`
- Re-query it through EF Core's standard pipeline

## How It Works

1. **SQL extraction**: Each `IQueryable<T>` is compiled to SQL via EF Core's `CreateDbCommand()` — a public, stable API available since EF Core 5.0 that returns a `DbCommand` with parameterized SQL without executing the query
2. **Batch assembly**: All commands are packed into a single `DbBatch` (ADO.NET's native batching abstraction, available in .NET 8+)
3. **Single execution**: The batch executes in one database round trip, returning a `DbDataReader` with multiple result sets
4. **Materialization**: Each result set is read sequentially via `NextResultAsync()` and materialized using EF Core's `IEntityType` metadata — column names, CLR types, and value converters are all respected
5. **Resolution**: Results are pushed through `TaskCompletionSource<T>`, resolving the futures returned to the caller

## Supported Providers

Any ADO.NET provider that supports `DbBatch` (.NET 8+) works with this API. Weasel tests against:

| Provider | Driver | Status |
|----------|--------|--------|
| PostgreSQL | Npgsql | Fully tested |
| SQL Server | Microsoft.Data.SqlClient | Fully tested |
| SQLite | Microsoft.Data.Sqlite | Supported |

There are no provider-specific differences in behavior. The same `BatchedQuery` code works identically across all three providers because the API operates entirely at the `System.Data.Common.DbBatch` abstraction level.

## Limitations

- **No change tracking**: Materialized entities are not tracked by EF Core's `ChangeTracker`. Use this for read-only queries.
- **Flat entity types**: The materializer handles entities with scalar properties, value converters, enums, and nullable columns. Complex owned types and navigation properties (includes) require loading through EF Core's standard query pipeline.
- **IQueryable only**: Queries must be expressible as `IQueryable<T>`. Raw SQL string queries are not yet supported in the batch API.
- **Single-use**: A `BatchedQuery` cannot be reused after `ExecuteAsync()` is called.
