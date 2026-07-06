namespace Weasel.Storage;

/// <summary>
///     Marker interface for storage operations whose SQL returns no result set, telling the
///     unit-of-work callback walker not to advance the batched reader for this operation.
/// </summary>
public interface NoDataReturnedCall
{
}
