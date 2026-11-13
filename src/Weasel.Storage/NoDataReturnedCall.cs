namespace Weasel.Storage;

/// <summary>
///     Marker interface for storage operations whose SQL returns no result set, telling the
///     unit-of-work callback walker not to advance the batched reader for this operation.
/// </summary>
/// <remarks>
///     <para>
///         The marker is a promise about the SQL, and the
///         <see cref="ResultSetWalk.SkipNoDataReturned" /> walk's alignment rests on it: a marked operation
///         that does return rows hands every later operation its neighbour's result set.
///         <see cref="OperationBatchOptions.SurplusResultSets" /> detects that after the fact.
///     </para>
///     <para>
///         An operation that wraps a marked one sees the marker through
///         <see cref="IStorageOperationWrapper" />; ask <see cref="OperationBatchExecutor.ReturnsResultSet" />
///         rather than type-testing for this interface.
///     </para>
/// </remarks>
public interface NoDataReturnedCall
{
}
