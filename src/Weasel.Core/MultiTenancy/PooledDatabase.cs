namespace Weasel.Core.MultiTenancy;

/// <summary>
/// Represents a database in the sharded tenancy pool
/// </summary>
public record PooledDatabase(
    string DatabaseId,
    string ConnectionString,
    bool IsFull,
    int TenantCount
);
