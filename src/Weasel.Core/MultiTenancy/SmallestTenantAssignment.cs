using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Weasel.Core.MultiTenancy;

/// <summary>
/// Assigns tenants to the database with the fewest tenants (or custom sizing metric).
/// Uses a pluggable <see cref="IDatabaseSizingStrategy"/> for determining "smallest".
/// Defaults to sorting by <see cref="PooledDatabase.TenantCount"/>.
/// </summary>
public class SmallestTenantAssignment : ITenantAssignmentStrategy
{
    private readonly IDatabaseSizingStrategy _sizingStrategy;

    public SmallestTenantAssignment(IDatabaseSizingStrategy? sizingStrategy = null)
    {
        _sizingStrategy = sizingStrategy ?? new TenantCountSizingStrategy();
    }

    public ValueTask<string> AssignTenantToDatabaseAsync(
        string tenantId, IReadOnlyList<PooledDatabase> availableDatabases)
    {
        if (availableDatabases.Count == 0)
        {
            throw new InvalidOperationException(
                "No available (non-full) databases in the pool to assign tenant to");
        }

        return _sizingStrategy.FindSmallestDatabaseAsync(availableDatabases);
    }
}

/// <summary>
/// Default sizing strategy that picks the database with the lowest tenant count.
/// </summary>
public class TenantCountSizingStrategy : IDatabaseSizingStrategy
{
    public ValueTask<string> FindSmallestDatabaseAsync(IReadOnlyList<PooledDatabase> databases)
    {
        var smallest = databases.OrderBy(d => d.TenantCount).First();
        return new ValueTask<string>(smallest.DatabaseId);
    }
}
