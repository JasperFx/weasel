using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Weasel.Core.MultiTenancy;

/// <summary>
/// Assigns tenants to databases using a deterministic hash of the tenant ID.
/// Uses a stable FNV-1a hash for consistent, well-distributed assignment.
/// </summary>
public class HashTenantAssignment : ITenantAssignmentStrategy
{
    public ValueTask<string> AssignTenantToDatabaseAsync(
        string tenantId, IReadOnlyList<PooledDatabase> availableDatabases)
    {
        if (availableDatabases.Count == 0)
        {
            throw new InvalidOperationException(
                "No available (non-full) databases in the pool to assign tenant to");
        }

        var hash = StableHash(tenantId);
        var index = (int)(hash % (uint)availableDatabases.Count);

        return new ValueTask<string>(availableDatabases[index].DatabaseId);
    }

    /// <summary>
    /// FNV-1a 32-bit hash — deterministic, fast, no external dependencies
    /// </summary>
    internal static uint StableHash(string value)
    {
        unchecked
        {
            uint hash = 2166136261;
            foreach (var c in value)
            {
                hash ^= c;
                hash *= 16777619;
            }
            return hash;
        }
    }
}
