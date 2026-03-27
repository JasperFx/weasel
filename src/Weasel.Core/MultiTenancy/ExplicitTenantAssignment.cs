using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Weasel.Core.MultiTenancy;

/// <summary>
/// Assignment strategy that always throws for unrecognized tenants.
/// Requires all tenants to be explicitly pre-assigned to a database
/// via the admin API before first use.
/// </summary>
public class ExplicitTenantAssignment : ITenantAssignmentStrategy
{
    public ValueTask<string> AssignTenantToDatabaseAsync(
        string tenantId, IReadOnlyList<PooledDatabase> availableDatabases)
    {
        throw new UnknownTenantIdException(tenantId);
    }
}

/// <summary>
/// Thrown when a tenant ID is not found in the tenant assignment table
/// and the assignment strategy does not support auto-assignment.
/// </summary>
public class UnknownTenantIdException : Exception
{
    public string TenantId { get; }

    public UnknownTenantIdException(string tenantId)
        : base($"Unknown tenant id '{tenantId}'. This tenant must be explicitly assigned to a database before use.")
    {
        TenantId = tenantId;
    }
}
