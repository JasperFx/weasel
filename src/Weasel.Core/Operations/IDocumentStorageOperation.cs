#nullable enable
using Weasel.Core.Operations.DirtyTracking;

namespace Weasel.Core.Operations;

public interface IDocumentStorageOperation: IStorageOperation
{
    object Document { get; }
    IChangeTracker ToTracker(IStorageSession session);
}
