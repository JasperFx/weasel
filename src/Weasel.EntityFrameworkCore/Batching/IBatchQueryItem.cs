using System.Data.Common;

namespace Weasel.EntityFrameworkCore.Batching;

/// <summary>
///     Internal interface for a single item queued in a <see cref="BatchedQuery" />.
///     Each item knows how to configure its batch command and read its result set.
/// </summary>
internal interface IBatchQueryItem
{
    /// <summary>
    ///     Configures a <see cref="DbBatchCommand" /> with the SQL and parameters
    ///     extracted from the original EF Core query.
    /// </summary>
    void ConfigureCommand(DbBatchCommand command);

    /// <summary>
    ///     Reads from the current result set in the <see cref="DbDataReader" />
    ///     and resolves the item's <see cref="TaskCompletionSource{T}" />.
    /// </summary>
    Task ReadAsync(DbDataReader reader, CancellationToken ct);
}
