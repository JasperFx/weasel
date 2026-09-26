using Microsoft.EntityFrameworkCore;

namespace Weasel.EntityFrameworkCore.Batching;

public static class BatchQueryExtensions
{
    /// <summary>
    ///     Creates a new <see cref="BatchedQuery" /> for combining multiple queries
    ///     into a single database round trip.
    /// </summary>
    public static BatchedQuery CreateBatchQuery(this DbContext context)
    {
        return new BatchedQuery(context);
    }

    /// <summary>
    ///     Registers <see cref="BatchedQueryInterceptor" /> so that <see cref="BatchedQuery" /> sends its
    ///     queries in a single round trip. Without it, each batched query runs on its own round trip.
    /// </summary>
    public static DbContextOptionsBuilder UseWeaselBatchedQueries(this DbContextOptionsBuilder builder)
    {
        return builder.AddInterceptors(BatchedQueryInterceptor.Instance);
    }

    /// <inheritdoc cref="UseWeaselBatchedQueries(DbContextOptionsBuilder)" />
    public static DbContextOptionsBuilder<TContext> UseWeaselBatchedQueries<TContext>(
        this DbContextOptionsBuilder<TContext> builder) where TContext : DbContext
    {
        return builder.AddInterceptors(BatchedQueryInterceptor.Instance);
    }
}
