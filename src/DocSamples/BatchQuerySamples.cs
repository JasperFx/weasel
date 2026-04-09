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
}
