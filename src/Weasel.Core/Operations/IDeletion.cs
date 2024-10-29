#nullable enable
namespace Weasel.Core.Operations;

public interface IDeletion: IStorageOperation, NoDataReturnedCall
{
    object Document { get; set; }
    object Id { get; set; }
}
