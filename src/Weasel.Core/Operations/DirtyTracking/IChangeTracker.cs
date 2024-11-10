namespace Weasel.Core.Operations.DirtyTracking;

public interface IChangeTracker
{
    object Document { get; }
    bool DetectChanges(IStorageSession session, out IStorageOperation operation);
    void Reset(IStorageSession session);
}
