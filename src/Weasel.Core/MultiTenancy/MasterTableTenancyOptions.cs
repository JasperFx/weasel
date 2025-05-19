using System.Data.Common;
using System.Reflection;
using JasperFx;
using JasperFx.Core;
using JasperFx.MultiTenancy;

namespace Weasel.Core.MultiTenancy;

public class MasterTableTenancyOptions<T> where T : DbDataSource
{
    private readonly IDatabaseProvider _provider;

    public MasterTableTenancyOptions(IDatabaseProvider provider)
    {
        _provider = provider;
        SchemaName = _provider.DefaultDatabaseSchemaName;
    }

    public readonly StaticConnectionStringSource SeedDatabases = new();

    /// <summary>
    ///     The connection string of the master database holding the tenant table
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    ///     A configured data source for managing the tenancy
    /// </summary>
    public T? DataSource { get; set; }

    /// <summary>
    ///     If specified, override the database schema name for the tenants table
    ///     Default is "public"
    /// </summary>
    public string SchemaName { get; set; }

    /// <summary>
    ///     Set an application name in the connection strings strictly for diagnostics
    /// </summary>
    // TODO -- make this fed from the JasperFxOptions
    public string ApplicationName { get; set; } = Assembly.GetEntryAssembly()?.FullName ?? "Marten";

    /// <summary>
    ///     If set, this will override the AutoCreate setting for just the master tenancy table
    /// </summary>
    public AutoCreate? AutoCreate { get; set; }

    public string CorrectedConnectionString()
    {
        if (ApplicationName.IsNotEmpty())
        {
            return _provider.AddApplicationNameToConnectionString(ConnectionString, ApplicationName);
        }

        return ConnectionString;
    }

    public string CorrectConnectionString(string connectionString)
    {
        if (ApplicationName.IsNotEmpty())
        {
            return _provider.AddApplicationNameToConnectionString(connectionString, ApplicationName);
        }

        return connectionString;
    }
}
