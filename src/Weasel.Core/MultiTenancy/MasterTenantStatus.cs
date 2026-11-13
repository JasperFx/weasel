#nullable enable
namespace Weasel.Core.MultiTenancy;

/// <summary>
/// What the master table says about one tenant id (weasel#809).
/// </summary>
/// <remarks>
/// <see cref="MasterTableTenancyBase{TDatabase,TDataSource}.TryFindDatabaseAsync" /> answers
/// <see cref="Unknown" /> and <see cref="Disabled" /> with the same null, because neither is routable.
/// They call for different actions, though — "never heard of it" against "switched off on purpose" —
/// and Marten, Polecat and Fisher each refuse a disabled tenant with <c>DisabledTenantException</c>, so
/// the base reports which it was through this.
/// </remarks>
public enum MasterTenantStatus
{
    /// <summary>No row in the control table for this tenant id.</summary>
    Unknown,

    /// <summary>A row exists and is disabled. Not routable until it is re-enabled.</summary>
    Disabled,

    /// <summary>A row exists, is enabled, and is routable.</summary>
    Enabled
}

/// <summary>
/// One tenant's row in the master table, read directly from it: its <see cref="Status" />, and its
/// connection string when — and only when — it is <see cref="MasterTenantStatus.Enabled" />.
/// </summary>
/// <param name="Status">Whether the tenant is unknown, disabled or enabled.</param>
/// <param name="ConnectionString">
/// The tenant's connection string with the application name applied, or null unless
/// <paramref name="Status" /> is <see cref="MasterTenantStatus.Enabled" />. Null for a disabled tenant
/// on purpose, so a caller that only null-checks cannot route to one.
/// </param>
public readonly record struct MasterTenantLookup(MasterTenantStatus Status, string? ConnectionString);

/// <summary>
/// One tenant resolved through the cache, falling back to the master table: its
/// <see cref="Status" />, and its database when — and only when — it is
/// <see cref="MasterTenantStatus.Enabled" />.
/// </summary>
/// <typeparam name="TDatabase">The store's own database type.</typeparam>
/// <param name="Status">Whether the tenant is unknown, disabled or enabled.</param>
/// <param name="Database">
/// The tenant's cached database, or null unless <paramref name="Status" /> is
/// <see cref="MasterTenantStatus.Enabled" />.
/// </param>
public readonly record struct MasterTenantResolution<TDatabase>(MasterTenantStatus Status, TDatabase? Database)
    where TDatabase : class;
