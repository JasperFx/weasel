#nullable enable
namespace Weasel.Storage;

/// <summary>
///     A storage operation that deletes a document, exposing the document and/or identity it
///     targets for unit-of-work bookkeeping. Deletions return no result set.
/// </summary>
public interface IDeletion: IStorageOperation, NoDataReturnedCall
{
    object Document { get; set; }
    object Id { get; set; }
}
