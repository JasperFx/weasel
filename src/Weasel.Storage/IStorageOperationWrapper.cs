#nullable enable
namespace Weasel.Storage;

/// <summary>
///     An operation that wraps another one — a store's adapter that binds a shared operation to a
///     session, or translates it into the store's own operation currency — and executes the wrapped
///     operation's SQL as its own.
/// </summary>
/// <remarks>
///     <para>
///         Whether a command contributes a result set is a fact about the SQL, so it belongs to the
///         wrapped operation. A wrapper cannot carry <see cref="NoDataReturnedCall" /> itself without
///         being wrong for half of what it wraps, and before this seam a wrapper around a no-data
///         operation hid the marker: the <see cref="ResultSetWalk.SkipNoDataReturned" /> walk then gave
///         it a result set it did not have, and every later operation read its neighbour's rows
///         (polecat#831). <see cref="OperationBatchExecutor.ReturnsResultSet" /> looks through this.
///     </para>
///     <para>
///         <see cref="OperationBatchExecutor" /> also looks through it for an
///         <see cref="JasperFx.Core.Exceptions.IExceptionTransform" />, when the wrapper is not one itself.
///     </para>
/// </remarks>
public interface IStorageOperationWrapper
{
    /// <summary>The operation whose SQL this one executes.</summary>
    IStorageOperation Inner { get; }
}
