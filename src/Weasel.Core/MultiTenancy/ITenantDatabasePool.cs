using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Weasel.Core.MultiTenancy;

/// <summary>
/// Abstraction for managing a pool of databases used in sharded multi-tenancy.
/// Implementations handle the persistence of database registry and tenant assignments.
/// </summary>
public interface ITenantDatabasePool
{
    /// <summary>
    /// List all databases in the pool with their current state
    /// </summary>
    ValueTask<IReadOnlyList<PooledDatabase>> ListDatabasesAsync(CancellationToken ct);

    /// <summary>
    /// Add a new database to the pool
    /// </summary>
    ValueTask AddDatabaseAsync(string databaseId, string connectionString, CancellationToken ct);

    /// <summary>
    /// Mark a database as full so no new tenants are assigned to it
    /// </summary>
    ValueTask MarkDatabaseFullAsync(string databaseId, CancellationToken ct);

    /// <summary>
    /// Find which database a tenant is assigned to, or null if not yet assigned
    /// </summary>
    ValueTask<string?> FindDatabaseForTenantAsync(string tenantId, CancellationToken ct);

    /// <summary>
    /// Assign a tenant to a specific database. Does not create partitions —
    /// that is the responsibility of the caller (e.g., Marten's ShardedTenancy).
    /// </summary>
    ValueTask AssignTenantAsync(string tenantId, string databaseId, CancellationToken ct);

    /// <summary>
    /// Remove a tenant assignment
    /// </summary>
    ValueTask RemoveTenantAsync(string tenantId, CancellationToken ct);
}
