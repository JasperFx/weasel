using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Weasel.Core;

namespace Weasel.EntityFrameworkCore;

/// <summary>
///     FK-aware database cleaner for EF Core DbContexts. Discovers tables from the
///     DbContext model metadata, sorts them in FK-safe deletion order, and generates
///     provider-specific SQL. The table graph and SQL are memoized on first use.
/// </summary>
public class DatabaseCleaner<TContext> : IDatabaseCleaner<TContext> where TContext : DbContext
{
    private readonly IServiceProvider _services;
    private string? _deleteAllSql;
    private Migrator? _migrator;
    private readonly object _lock = new();

    public DatabaseCleaner(IServiceProvider services)
    {
        _services = services;
    }

    private (string sql, Migrator migrator) EnsureInitialized()
    {
        if (_deleteAllSql != null && _migrator != null)
        {
            return (_deleteAllSql, _migrator);
        }

        lock (_lock)
        {
            if (_deleteAllSql != null && _migrator != null)
            {
                return (_deleteAllSql, _migrator);
            }

            using var scope = _services.CreateScope();
            var context = scope.ServiceProvider.GetRequiredService<TContext>();

            var (_, migrator) = scope.ServiceProvider.FindMigratorForDbContext(context);
            _migrator = migrator!;

            // GetEntityTypesForMigration returns parent-first (creation order).
            // Reverse for child-first (deletion order).
            var entityTypes = DbContextExtensions.GetEntityTypesForMigration(context);
            var tables = new List<DbObjectName>();

            foreach (var entityType in entityTypes.Reverse())
            {
                var tableName = entityType.GetTableName();
                var schema = entityType.GetSchema() ?? _migrator.DefaultSchemaName;
                if (tableName != null)
                {
                    tables.Add(_migrator.Provider.Parse(schema, tableName));
                }
            }

            _deleteAllSql = _migrator.GenerateDeleteAllSql(tables);
            return (_deleteAllSql, _migrator);
        }
    }

    /// <inheritdoc />
    public async Task DeleteAllDataAsync(CancellationToken ct = default)
    {
        using var scope = _services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TContext>();
        var conn = context.Database.GetDbConnection();
        await DeleteAllDataAsync(conn, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task DeleteAllDataAsync(DbConnection connection, CancellationToken ct = default)
    {
        var (sql, _) = EnsureInitialized();
        if (string.IsNullOrEmpty(sql)) return;

        var controlled = false;
        if (connection.State != ConnectionState.Open)
        {
            controlled = true;
            await connection.OpenAsync(ct).ConfigureAwait(false);
        }

        try
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            if (controlled)
            {
                await connection.CloseAsync().ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc />
    public async Task ResetAllDataAsync(CancellationToken ct = default)
    {
        using var scope = _services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TContext>();
        var conn = context.Database.GetDbConnection();

        await DeleteAllDataAsync(conn, ct).ConfigureAwait(false);
        await RunSeedersAsync(scope.ServiceProvider, context, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task ResetAllDataAsync(DbConnection connection, CancellationToken ct = default)
    {
        await DeleteAllDataAsync(connection, ct).ConfigureAwait(false);

        // For multi-tenant: create a new scope and configure the context to use the supplied connection
        using var scope = _services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TContext>();
        context.Database.SetDbConnection(connection);

        await RunSeedersAsync(scope.ServiceProvider, context, ct).ConfigureAwait(false);
    }

    private static async Task RunSeedersAsync(IServiceProvider scopedServices, TContext context, CancellationToken ct)
    {
        var seeders = scopedServices.GetServices<IInitialData<TContext>>();
        foreach (var seeder in seeders)
        {
            await seeder.Populate(context, ct).ConfigureAwait(false);
        }
    }
}
