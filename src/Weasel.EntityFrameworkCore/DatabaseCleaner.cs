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
    private IReadOnlyList<DbObjectName>? _tables;
    private Migrator? _migrator;
    private readonly object _lock = new();

    public DatabaseCleaner(IServiceProvider services)
    {
        _services = services;
    }

    private (IReadOnlyList<DbObjectName> tables, Migrator migrator) EnsureInitialized()
    {
        if (_tables != null && _migrator != null)
        {
            return (_tables, _migrator);
        }

        lock (_lock)
        {
            if (_tables != null && _migrator != null)
            {
                return (_tables, _migrator);
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

            _tables = tables;
            return (_tables, _migrator);
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
        var (tables, migrator) = EnsureInitialized();
        if (tables.Count == 0) return;

        var controlled = false;
        if (connection.State != ConnectionState.Open)
        {
            controlled = true;
            await connection.OpenAsync(ct).ConfigureAwait(false);
        }

        try
        {
            // Generated per call rather than memoized with the table graph: a provider may have to read
            // the database to know what it can emit, and the answer can change between calls -- SQLite's
            // sqlite_sequence appears the first time anything is declared AUTOINCREMENT (weasel#546).
            // The reflection over the EF model, which is the expensive part, stays cached.
            var sql = await migrator
                .GenerateDeleteAllSqlAsync(connection, tables, ct: ct)
                .ConfigureAwait(false);

            if (string.IsNullOrEmpty(sql)) return;

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
