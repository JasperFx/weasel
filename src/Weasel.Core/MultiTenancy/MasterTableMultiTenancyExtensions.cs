using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Weasel.Core.MultiTenancy;

public interface IMasterTableMultiTenancy
{
    Task<bool> TryAddTenantDatabaseRecordsAsync(string tenantId, string connectionString);
    Task<bool> ClearAllDatabaseRecordsAsync();
}

public static class MasterTableMultiTenancyExtensions
{
    /// <summary>
    /// Convenience method to clear all tenant database records
    /// if using any CritterStack flavor of "master table tenancy"
    /// </summary>
    /// <param name="host"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static async Task ClearAllTenantDatabaseRecordsAsync(this IHost host)
    {
        var tenancyModels = host.Services.GetServices<IMasterTableMultiTenancy>().ToArray();
        if (!tenancyModels.Any())
        {
            throw new InvalidOperationException($"No {nameof(IMasterTableMultiTenancy)} services are registered.");
        }

        bool applied = false;
        foreach (var tenancyModel in tenancyModels)
        {
            applied = applied || await tenancyModel.ClearAllDatabaseRecordsAsync().ConfigureAwait(false);
        }

        if (!applied)
        {
            throw new InvalidOperationException($"No {nameof(IMasterTableMultiTenancy)} services could actively apply this change.");
        }
    }

    /// <summary>
    /// Convenience method to add a new tenant database to the master tenant table at runtime when using any
    /// Critter Stack version of "master table tenancy"
    /// </summary>
    /// <param name="host"></param>
    /// <param name="tenantId"></param>
    /// <param name="connectionString"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static async Task AddTenantDatabaseAsync(this IHost host, string tenantId, string connectionString)
    {
        var tenancyModels = host.Services.GetServices<IMasterTableMultiTenancy>().ToArray();
        if (!tenancyModels.Any())
        {
            throw new InvalidOperationException($"No {nameof(IMasterTableMultiTenancy)} services are registered.");
        }

        bool applied = false;
        foreach (var tenancyModel in tenancyModels)
        {
            applied = applied || await tenancyModel.TryAddTenantDatabaseRecordsAsync(tenantId, connectionString).ConfigureAwait(false);
        }

        if (!applied)
        {
            throw new InvalidOperationException($"No {nameof(IMasterTableMultiTenancy)} services could actively apply this change.");
        }
    }
}
