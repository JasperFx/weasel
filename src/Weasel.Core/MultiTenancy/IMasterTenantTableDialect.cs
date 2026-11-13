#nullable enable
namespace Weasel.Core.MultiTenancy;

/// <summary>
/// The SQL half of master-table multi-tenancy. Everything about the control table that is not
/// dialect-specific — the cache, the guarded provisioning, the runtime tenant lifecycle — lives in
/// <see cref="MasterTableTenancyBase{TDatabase,TDataSource}" />; this is the part that has to know
/// whether an upsert spells itself <c>on conflict … do update</c> or <c>MERGE</c>, and whether a
/// boolean is <c>false</c> or <c>0</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two hard contracts, because the base binds and reads positionally.</b>
/// </para>
/// <list type="number">
/// <item>
/// Every statement that takes a tenant id names the parameter <c>@id</c>, and
/// <see cref="UpsertTenant" /> additionally names <c>@connection</c>. Npgsql,
/// Microsoft.Data.SqlClient and Microsoft.Data.Sqlite all accept <c>@</c>-prefixed names, so one
/// spelling serves every dialect the Critter Stack ships.
/// </item>
/// <item>
/// <see cref="SelectEnabledTenants" /> returns <c>tenant_id</c> at ordinal 0 and
/// <c>connection_string</c> at ordinal 1; <see cref="SelectDisabledTenantIds" /> and
/// <see cref="SelectConnectionString" /> return a single column at ordinal 0;
/// <see cref="SelectTenantRow" /> returns <c>connection_string</c> at ordinal 0 and the disabled
/// flag at ordinal 1.
/// </item>
/// </list>
/// </remarks>
public interface IMasterTenantTableDialect
{
    /// <summary>
    /// The control table's name, qualified and quoted the way this dialect wants it — e.g.
    /// <c>public.mt_tenant_databases</c> or <c>[dbo].[pc_tenants]</c>. Every other member is
    /// handed the result of this, so the qualification rule lives in one place.
    /// </summary>
    string QualifiedTableName(string schemaName, string tableName);

    /// <summary>
    /// Idempotent DDL creating the schema (if the dialect has schemas) and the control table.
    /// </summary>
    /// <remarks>
    /// It has to be idempotent rather than guarded by a prior existence check, because two
    /// processes can reach it at once — the semaphore in the base guards one process, not a
    /// deployment. A store that would rather run this through Weasel's migration pipeline should
    /// override <see cref="MasterTableTenancyBase{TDatabase,TDataSource}.ProvisionControlTableAsync" />
    /// instead, in which case this member is never called.
    /// </remarks>
    string CreateControlTable(string schemaName, string tableName, string qualifiedTableName);

    /// <summary>
    /// <c>tenant_id, connection_string</c> for every tenant that is not disabled.
    /// </summary>
    string SelectEnabledTenants(string qualifiedTableName);

    /// <summary>
    /// <c>tenant_id</c> for every disabled tenant.
    /// </summary>
    string SelectDisabledTenantIds(string qualifiedTableName);

    /// <summary>
    /// <c>connection_string</c> for one enabled tenant (<c>@id</c>), or no row.
    /// </summary>
    /// <remarks>
    /// <b>The "and not disabled" half is not optional.</b> Without it a disabled tenant resolves
    /// and opens sessions, which is the one thing disabling is for — and it would do so silently,
    /// because everything else about the tenant is intact.
    /// </remarks>
    string SelectConnectionString(string qualifiedTableName);

    /// <summary>
    /// <c>connection_string</c> at ordinal 0 and the disabled flag at ordinal 1 for one tenant
    /// (<c>@id</c>) <b>whether or not it is disabled</b>, or no row — so one read can tell
    /// "no such tenant" from "that tenant is switched off" (weasel#809). Or null when this dialect
    /// does not render it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the one statement that must <em>not</em> filter on the disabled flag, unlike
    /// <see cref="SelectConnectionString" />: the base reads the flag and keeps a disabled tenant's
    /// connection string to itself, so nothing routes to it. The flag may come back as any type
    /// <see cref="Convert.ToBoolean(object)" /> accepts — a <c>boolean</c>, a <c>BIT</c> or an
    /// integer.
    /// </para>
    /// <para>
    /// <b>The default is null rather than a guess</b>, because this interface cannot know a
    /// dialect's column names, and null is an answer a stale implementer cannot be mistaken for
    /// giving on purpose. A dialect that leaves it null still distinguishes disabled from unknown —
    /// the base falls back to <see cref="SelectConnectionString" /> and, only when that finds
    /// nothing, <see cref="SelectDisabledTenantIds" /> — at the cost of a second round trip on a miss.
    /// </para>
    /// </remarks>
    string? SelectTenantRow(string qualifiedTableName) => null;

    /// <summary>
    /// Insert or update a tenant's connection string (<c>@id</c>, <c>@connection</c>).
    /// </summary>
    /// <remarks>
    /// An upsert rather than an insert, so re-adding a tenant is idempotent. Whether it also clears
    /// the disabled flag is the dialect's call and the two shipped stores disagree — Polecat's
    /// MERGE re-enables, Marten's <c>on conflict</c> does not — so the base does not assume either;
    /// callers wanting a re-enable can say so.
    /// </remarks>
    string UpsertTenant(string qualifiedTableName);

    /// <summary>Delete one tenant's record (<c>@id</c>).</summary>
    string DeleteTenant(string qualifiedTableName);

    /// <summary>Delete every tenant record.</summary>
    string DeleteAllTenants(string qualifiedTableName);

    /// <summary>Set or clear one tenant's disabled flag (<c>@id</c>).</summary>
    string SetTenantDisabled(string qualifiedTableName, bool disabled);
}
