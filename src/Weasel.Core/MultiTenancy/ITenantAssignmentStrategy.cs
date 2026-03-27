using System.Collections.Generic;
using System.Threading.Tasks;

namespace Weasel.Core.MultiTenancy;

/// <summary>
/// Strategy for determining which database a new tenant should be assigned to.
/// Implementations are called under an advisory lock, so they do not need to
/// handle concurrency themselves.
/// </summary>
public interface ITenantAssignmentStrategy
{
    /// <summary>
    /// Determine which database a new tenant should be assigned to.
    /// </summary>
    /// <param name="tenantId">The tenant being assigned</param>
    /// <param name="availableDatabases">Non-full databases in the pool</param>
    /// <returns>The database_id to assign the tenant to</returns>
    /// <exception cref="System.InvalidOperationException">
    /// Thrown if no suitable database is available
    /// </exception>
    ValueTask<string> AssignTenantToDatabaseAsync(
        string tenantId, IReadOnlyList<PooledDatabase> availableDatabases);
}
