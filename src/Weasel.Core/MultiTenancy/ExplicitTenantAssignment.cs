using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JasperFx.MultiTenancy;

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
