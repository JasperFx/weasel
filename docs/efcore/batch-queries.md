# Batch Queries

Weasel provides a batch query API for EF Core that combines multiple queries into a single database round trip. This is similar to [Marten's IBatchedQuery](https://martendb.io/documents/querying/batched-queries) and addresses a long-standing EF Core feature request ([dotnet/efcore#10879](https://github.com/dotnet/efcore/issues/10879)).

::: tip
If you're using **Wolverine** with EF Core, Wolverine can auto-batch queries inside your message handlers. See [Wolverine's batch query documentation](https://wolverinefx.io/guide/durability/efcore/batch-queries.html) for handler-specific patterns. This page covers the underlying `BatchedQuery` fluent API that Wolverine builds on.
:::

## Why Batch?

Every database query is a network round trip. If a request handler needs three queries, that's three round trips. Batching combines them into one, which can be the single biggest performance improvement for database-heavy endpoints.

In benchmarks on a local SQL Server with 4 keyed lookups per handler invocation, batching delivers a **2.78× speedup** (6.92 ms → 2.49 ms per handler). The improvement scales with the number of queries and with network latency — across a region-to-region hop, a four-query handler can drop from ~40 ms to ~12 ms.

## API Reference

`BatchedQuery` exposes three query methods. Each queues the `IQueryable<T>` and returns a `Task<T>` future that is resolved when `ExecuteAsync()` is called. EF Core prepares the query when it is queued, so the values it captures are read then, once, and a query EF Core can't translate throws right away. Results are exactly what EF Core returns for the same query, including entities with owned, complex or JSON members, `Include`s and projections.

| Method | Returns | Description |
|--------|---------|-------------|
| `Query<T>(IQueryable<T>)` | `Task<IReadOnlyList<T>>` | Returns all results as a list. |
| `QuerySingle<T>(IQueryable<T>)` | `Task<T?>` | Returns the first result, or the default value (`null` for an entity) if there is none. |
| `Scalar<T>(IQueryable<T>)` | `Task<T>` | Returns a single scalar value (e.g., from a COUNT or MAX projection), or the default value if there is none. |
| `ExecuteAsync(CancellationToken)` | `Task` | Sends all queued queries in one round trip and resolves every future. |

The `DbContext.CreateBatchQuery()` extension method creates a new `BatchedQuery` bound to that context's connection and transaction.

## Setup

To send the queries in a single round trip, register Weasel's `BatchedQueryInterceptor` on the `DbContext` with `UseWeaselBatchedQueries()`:

<!-- snippet: sample_efcore_batch_query_registration -->
<a id='snippet-sample_efcore_batch_query_registration'></a>
```cs
var options = new DbContextOptionsBuilder<ShopDbContext>()
    .UseNpgsql(connectionString)
    // Lets BatchedQuery send all its queries in one round trip
    .UseWeaselBatchedQueries()
    .Options;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/BatchQuerySamples.cs#L120-L126' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_batch_query_registration' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The same call works inside `services.AddDbContext<T>(options => ...)` or a `DbContext`'s `OnConfiguring`.

`BatchedQuery` runs every query on its own round trip instead, with the same results, when a batch can't reproduce what EF Core would do:

- the interceptor isn't registered,
- the database provider doesn't support `DbBatch` (`DbConnection.CanCreateBatch` is false, as for SQLite and Oracle),
- other command interceptors are registered (a batch can't apply their changes to commands),
- the execution strategy retries on failure, including when the queries are queued or the batch runs inside a retrying strategy's `ExecuteAsync()` (a query read from a batch can't be retried on its own),
- the version of EF Core doesn't provide a prepared query's command (see [How It Works](#how-it-works)), or
- one of the queued queries is a split query as EF Core compiles it, whether through `AsSplitQuery()`, `UseQuerySplittingBehavior()` or a query interceptor (running it separately would change the order the queries run in).

It logs a warning once per `DbContext` type for the first three.

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

// 1. Queue phase — EF Core prepares each query, reading the values it captures,
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

A batch runs on the `DbContext` it was created for, so it only accepts that context's queries. Queuing a query built from another `DbContext` throws.

## Error Handling

If any query in the batch fails (e.g., a SQL syntax error or connection failure), `ExecuteAsync()` throws. The futures of queries that didn't complete are faulted, so awaiting them after a failed `ExecuteAsync()` throws rather than waiting forever.

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

**Captured values**: EF Core reads the values a query captures once, when the query is queued, whether or not it ends up in a batch, so changing a variable afterwards doesn't change the query. EF Core applies its usual rules: expressions it translates to SQL, such as `DateTime.UtcNow`, use the database's value, and branches a condition rules out aren't evaluated.

**Collections in `Contains()`**: EF Core enumerates a collection the query captures each time it builds a command for the query, not when it reads the query's other values. A batched query's command is built more than once (for the batch, to detect a split query, and when EF Core reads the query's result set), so pass a list or array rather than a deferred sequence, such as an iterator or LINQ query over objects, that is expensive or can only be enumerated once.

**Independence**: Each query in the batch is independent. Results from one query cannot feed into another within the same batch. If you need dependent queries, execute the first batch, await the result, then build a second batch.

**Thread safety**: `BatchedQuery` is **not thread-safe**. All `Query`/`QuerySingle`/`Scalar` calls and the `ExecuteAsync` call must happen on the same async context (which is the natural pattern in request handlers and test methods).

**Transaction awareness**: If the `DbContext` has an active transaction (`Database.CurrentTransaction`), the batch executes within that transaction.

## Change Tracking

Batched queries follow EF Core's tracking rules, as if each query ran on its own. A tracking query (the default, or `AsTracking()`) attaches its results to the `ChangeTracker` and returns the instance the context already tracks for an entity it has loaded before; an `AsNoTracking()` query or a context configured with `QueryTrackingBehavior.NoTracking` returns untracked results.

## How It Works

1. **Preparation**: When a query is queued, EF Core prepares it once (`IAsyncQueryProvider.ExecuteAsync()`): it reads the values the query captures, compiles it, and returns the query ready to run. Weasel takes the query's SQL and parameters from that prepared query through `IRelationalQueryingEnumerable.CreateDbCommand()`. That interface is internal to EF Core, unchanged since EF Core 5.0; Weasel uses it in one place because the public `CreateDbCommand()` would prepare the query again and read its captured values a second time. If a version of EF Core doesn't provide it, `BatchedQuery` runs each query on its own round trip.
2. **Batch assembly**: All commands are packed into a single `DbBatch` (ADO.NET's native batching abstraction, available in .NET 8+)
3. **Single execution**: The batch executes in one database round trip, returning a `DbDataReader` with multiple result sets
4. **Materialization**: For each result set in turn, Weasel runs the prepared query through EF Core as usual. `BatchedQueryInterceptor`, an EF Core `DbCommandInterceptor`, recognizes the query's command and hands EF Core the provider's reader, positioned on that result set, instead of executing it (and keeps EF Core from closing it), so EF Core materializes the rows itself — tracking, identity resolution, owned, complex and JSON members, `Include`s and projections behave exactly as they do outside a batch
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

- **Batching isn't always possible**: Without `UseWeaselBatchedQueries()`, on a provider without `DbBatch` support, with other command interceptors, with a retrying execution strategy, or with a split query in the batch, every query runs on its own round trip (see [Setup](#setup)).
- **IQueryable only**: Queries must be expressible as `IQueryable<T>`. Raw SQL string queries are not yet supported in the batch API.
- **Single-use**: A `BatchedQuery` cannot be reused after `ExecuteAsync()` is called.
