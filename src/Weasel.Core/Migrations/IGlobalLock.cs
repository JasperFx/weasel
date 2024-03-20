using System.Data.Common;

namespace Weasel.Core.Migrations;

public interface IGlobalLock<in TConnection> where TConnection : DbConnection
{
    Task<AttainLockResult> TryAttainLock(TConnection conn, CancellationToken ct = default);
    Task ReleaseLock(TConnection conn, CancellationToken ct = default);
}