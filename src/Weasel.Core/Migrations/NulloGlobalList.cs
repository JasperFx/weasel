using System.Data.Common;

namespace Weasel.Core.Migrations;

internal class NulloGlobalList<TConnection>: IGlobalLock<TConnection> where TConnection : DbConnection
{
    public Task<AttainLockResult> TryAttainLock(TConnection conn, CancellationToken ct = default)
    {
        return Task.FromResult(AttainLockResult.Success);
    }

    public Task ReleaseLock(TConnection conn, CancellationToken ct = default)
    {
        return Task.CompletedTask;
    }
}