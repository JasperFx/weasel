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
}
