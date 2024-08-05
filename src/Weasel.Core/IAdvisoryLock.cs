namespace Weasel.Core;

internal interface IAdvisoryLock: IAsyncDisposable
{
    bool HasLock(int lockId);
    Task<bool> TryAttainLockAsync(int lockId, CancellationToken token);
    Task ReleaseLockAsync(int lockId);
}
