using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Core.MultiTenancy;

namespace DocSamples;

#region sample_ITenantDatabasePool_interface
public interface ITenantDatabasePool_Sample
{
    ValueTask<IReadOnlyList<PooledDatabase>> ListDatabasesAsync(CancellationToken ct);
    ValueTask AddDatabaseAsync(string databaseId, string connectionString, CancellationToken ct);
    ValueTask MarkDatabaseFullAsync(string databaseId, CancellationToken ct);
    ValueTask<string?> FindDatabaseForTenantAsync(string tenantId, CancellationToken ct);
    ValueTask AssignTenantAsync(string tenantId, string databaseId, CancellationToken ct);
    ValueTask RemoveTenantAsync(string tenantId, CancellationToken ct);
}
#endregion

#region sample_ITenantAssignmentStrategy_interface
public interface ITenantAssignmentStrategy_Sample
{
    ValueTask<string> AssignTenantToDatabaseAsync(
        string tenantId,
        IReadOnlyList<PooledDatabase> availableDatabases);
}
#endregion

#region sample_IDatabaseSizingStrategy_interface
public interface IDatabaseSizingStrategy_Sample
{
    ValueTask<string> FindSmallestDatabaseAsync(IReadOnlyList<PooledDatabase> databases);
}
#endregion

public class MultiTenancySamples
{
    #region sample_IDatabase_TenantIds
    public interface IDatabase_TenantIds_Sample
    {
        List<string> TenantIds { get; }
        // ... other members
    }
    #endregion
}
