using System.Collections.Generic;
using System.Threading.Tasks;

namespace Weasel.Core.MultiTenancy;

/// <summary>
/// Pluggable strategy for determining which database in a pool is the "smallest"
/// and should receive the next tenant assignment. The default implementation
/// uses tenant count, but implementations could query actual row counts,
/// disk usage, or other metrics.
/// </summary>
public interface IDatabaseSizingStrategy
{
    /// <summary>
    /// Find the smallest database from the available (non-full) databases
    /// </summary>
    ValueTask<string> FindSmallestDatabaseAsync(IReadOnlyList<PooledDatabase> databases);
}
