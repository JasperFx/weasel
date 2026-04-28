using Microsoft.Extensions.Logging;
using Npgsql;
using Weasel.Core;

namespace Weasel.Postgresql;

public static class AdvisoryLockFactory
{
    public static IAdvisoryLock Create(NpgsqlDataSource dataSource, ILogger logger, string databaseName, AdvisoryLockOptions options)
    {
        return options.LockMonitoringEnabled
            ? new AdvisoryLock(dataSource, logger, databaseName, options)
            : new NativeAdvisoryLock(dataSource, logger, databaseName);
    }
}
