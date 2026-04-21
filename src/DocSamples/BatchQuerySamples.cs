using Microsoft.EntityFrameworkCore;
using Weasel.EntityFrameworkCore.Batching;

namespace DocSamples;

public class BatchQuerySamples
{
    public async Task basic_batch_query(ShopDbContext context)
    {
        #region sample_efcore_batch_query_basic
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
        #endregion
    }

    public async Task single_entity_query(ShopDbContext context)
    {
        #region sample_efcore_batch_query_single
        await using var batch = context.CreateBatchQuery();

        // QuerySingle returns a single entity or null
        var customerTask = batch.QuerySingle(
            context.Customers.Where(c => c.Id == 42));

        var orderTask = batch.QuerySingle(
            context.Orders.Where(o => o.Id == 100));

        await batch.ExecuteAsync();

        var customer = await customerTask; // may be null
        var order = await orderTask;
        #endregion
    }

    public async Task mixed_queries(ShopDbContext context)
    {
        #region sample_efcore_batch_query_mixed
        await using var batch = context.CreateBatchQuery();

        // Mix list queries, single entity lookups, and filtered queries
        var allCustomers = batch.Query(context.Customers);
        var pendingOrders = batch.Query(
            context.Orders.Where(o => o.Status == "Pending").OrderBy(o => o.Id));
        var specificCustomer = batch.QuerySingle(
            context.Customers.Where(c => c.Id == 1));

        // All three execute in a single round trip
        await batch.ExecuteAsync();
        #endregion
    }

    public async Task lifecycle_example(ShopDbContext context)
    {
        #region sample_efcore_batch_lifecycle
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
        #endregion
    }

    public async Task error_handling(ShopDbContext context)
    {
        #region sample_efcore_batch_error_handling
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
        #endregion
    }
}
