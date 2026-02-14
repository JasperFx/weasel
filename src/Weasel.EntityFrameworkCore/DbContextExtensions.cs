using System.Data;
using System.Data.Common;
using JasperFx;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Weasel.Core;
using Weasel.Core.Migrations;
using ITable = Weasel.Core.ITable;

namespace Weasel.EntityFrameworkCore;

public record DbContextMigration(DbConnection Connection, Migrator Migrator, SchemaMigration Migration) : IAsyncDisposable
{
    public async Task ExecuteAsync(AutoCreate autoCreate, CancellationToken cancellation, IMigrationLogger? logger = null)
    {
        if (autoCreate == AutoCreate.None) return;
        if (Migration.Difference == SchemaPatchDifference.None) return;

        var controlled = false;
        if (Connection.State != ConnectionState.Open)
        {
            controlled = true;
            await Connection.OpenAsync(cancellation).ConfigureAwait(false);
        }

        try
        {
            await Migrator.ApplyAllAsync(Connection, Migration, autoCreate, ct: cancellation, logger:logger).ConfigureAwait(false);
        }
        finally
        {
            if (controlled)
            {
                await Connection.CloseAsync().ConfigureAwait(false);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Connection.DisposeAsync().ConfigureAwait(false);
    }
}

public static class DbContextExtensions
{
    public static IDatabaseWithTables CreateDatabase(this IServiceProvider services, DbContext context, string? identifier = null)
    {
        identifier ??= context.GetType().FullNameInCode();
        var (originalConn, migrator) = services.FindMigratorForDbContext(context);

        // Prefer data source path to preserve authentication credentials
        var dataSource = FindDataSource(context);
        IDatabaseWithTables database;
        DbConnection? ownedConn = null;

        if (dataSource != null)
        {
            database = migrator!.CreateDatabase(dataSource, identifier);
        }
        else
        {
            ownedConn = GetConnectionWithCredentials(context, originalConn);
            database = migrator!.CreateDatabase(ownedConn, identifier);
        }

        try
        {
            foreach (var entityType in GetEntityTypesForMigration(context))
            {
                var table = migrator.MapToTable(entityType);
                database.AddTable(table);
            }

            return database;
        }
        finally
        {
            ownedConn?.Dispose();
        }
    }

    public static async Task<DbContextMigration> CreateMigrationAsync(
        this IServiceProvider services,
        DbContext context,
        CancellationToken cancellation)
    {
        var (originalConn, migrator) = services.FindMigratorForDbContext(context);
        var conn = GetConnectionWithCredentials(context, originalConn);

        var tables = GetEntityTypesForMigration(context)
            .Select(x => migrator!.MapToTable(x))
            .OfType<ISchemaObject>()
            .ToArray();

        await conn.OpenAsync(cancellation).ConfigureAwait(false);

        try
        {
            var migration = await SchemaMigration.DetermineAsync(conn, cancellation, tables).ConfigureAwait(false);
            return new DbContextMigration(conn, migrator, migration);
        }
        finally
        {
            await conn.CloseAsync().ConfigureAwait(false);
        }
    }

    public static (DbConnection conn, Migrator? migrator) FindMigratorForDbContext(this IServiceProvider services, DbContext context)
    {
        var migrators = services.GetServices<Migrator>().ToList();
        if (!migrators.Any())
        {
            throw new InvalidOperationException($"No {typeof(Migrator).FullNameInCode()} services are registered!");
        }

        var conn = context.Database.GetDbConnection();
        var migrator = migrators.FirstOrDefault(x => x.MatchesConnection(conn));
        if (migrator == null)
        {
            throw new InvalidOperationException(
                $"No matching {typeof(Migrator).FullNameInCode()} instances for DbContext {context}. Registered migrators are {migrators.Select(x => x.ToString() ?? x.GetType().Name).Join(", ")}");
        }

        return (conn, migrator);
    }

    /// <summary>
    /// Finds the DbDataSource configured on a DbContext via EF Core options extensions.
    /// Returns null if the context was not configured with a data source.
    /// </summary>
    internal static DbDataSource? FindDataSource(DbContext context)
    {
        var dbContextOptions = context.GetService<IDbContextOptions>();
        foreach (var ext in dbContextOptions.Extensions)
        {
            var dsProperty = ext.GetType().GetProperty("DataSource");
            if (dsProperty != null && typeof(DbDataSource).IsAssignableFrom(dsProperty.PropertyType))
            {
                if (dsProperty.GetValue(ext) is DbDataSource dataSource)
                {
                    return dataSource;
                }
            }
        }

        return null;
    }

    /// <summary>
    /// Gets a connection with full credentials for use in Weasel migrations.
    /// After EnsureCreatedAsync() or when using DbDataSource, the connection's
    /// ConnectionString property may strip credentials.
    /// </summary>
    internal static DbConnection GetConnectionWithCredentials(DbContext context, DbConnection originalConn)
    {
        // 1. Try the DbContext's configured connection string
        var connectionString = context.Database.GetConnectionString();

        // 2. If the connection string is missing or lacks credentials, try the data source
        if (connectionString == null || !HasCredentials(connectionString))
        {
            var dataSource = FindDataSource(context);
            if (dataSource != null)
            {
                return dataSource.CreateConnection();
            }
        }

        // For non-data-source connections, restore the original connection string
        if (connectionString != null && originalConn.State == ConnectionState.Closed)
        {
            originalConn.ConnectionString = connectionString;
        }

        return originalConn;
    }

    private static bool HasCredentials(string connectionString)
    {
        return connectionString.Contains("Password", StringComparison.OrdinalIgnoreCase)
            || connectionString.Contains("Pwd", StringComparison.OrdinalIgnoreCase)
            || connectionString.Contains("Integrated Security", StringComparison.OrdinalIgnoreCase)
            || connectionString.Contains("Trusted_Connection", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Gets entity types eligible for Weasel migration, filtering out those
    /// excluded from migrations via EF Core's ExcludeFromMigrations().
    /// Uses the design-time model to access migration annotations.
    /// </summary>
    public static IEnumerable<IEntityType> GetEntityTypesForMigration(DbContext context)
    {
        // The design-time model has full annotation access including IsTableExcludedFromMigrations.
        // The default read-optimized model throws when accessing migration-related annotations.
        var designTimeModel = context.GetService<IDesignTimeModel>();
        var model = designTimeModel?.Model ?? context.Model;

        return model.GetEntityTypes().Where(e => !e.IsTableExcludedFromMigrations());
    }

    public static ITable MapToTable(this Migrator migrator, IEntityType entityType)
    {
        var tableName = entityType.GetTableName();
        var efSchema = entityType.GetSchema(); // EF Core's schema (may be null)
        var tableSchema = efSchema ?? migrator.DefaultSchemaName; // Resolved schema for table creation

        if (tableName == null)
        {
            throw new InvalidOperationException($"Entity type {entityType.Name} does not have a table name configured.");
        }

        var identifier = migrator.Provider.Parse(tableSchema, tableName);
        var table = migrator.CreateTable(identifier);

        // Use EF Core's schema (not resolved) for StoreObjectIdentifier to match EF Core's internal mappings
        var storeObjectIdentifier = StoreObjectIdentifier.Table(tableName, efSchema);

        // Get primary key columns
        var primaryKey = entityType.FindPrimaryKey();
        var primaryKeyPropertyNames = primaryKey?.Properties
            .Select(p => p.Name)
            .ToHashSet() ?? [];

        // Add columns from properties
        foreach (var property in entityType.GetProperties())
        {
            mapColumn(property, storeObjectIdentifier, primaryKeyPropertyNames, table);
        }

        // Set primary key constraint name from EF Core metadata
        if (primaryKey != null)
        {
            var pkName = primaryKey.GetName(storeObjectIdentifier);
            if (pkName != null)
            {
                table.PrimaryKeyName = pkName;
            }
        }

        // Add foreign keys
        foreach (var foreignKey in entityType.GetForeignKeys())
        {
            mapForeignKey(migrator, foreignKey, storeObjectIdentifier, table);
        }

        return table;
    }

    private static void mapForeignKey(Migrator migrator, IForeignKey foreignKey,
        StoreObjectIdentifier storeObjectIdentifier, ITable table)
    {
        var principalEntityType = foreignKey.PrincipalEntityType;
        var principalTableName = principalEntityType.GetTableName();
        var principalEfSchema = principalEntityType.GetSchema(); // EF Core's schema (may be null)
        var principalTableSchema = principalEfSchema ?? migrator.DefaultSchemaName; // Resolved for table identifier

        if (principalTableName == null) return;

        var principalIdentifier = migrator.Provider.Parse(principalTableSchema, principalTableName);

        // Use EF Core's raw schema for StoreObjectIdentifier to match EF Core's internal mappings
        var principalStoreObjectIdentifier = StoreObjectIdentifier.Table(principalTableName, principalEfSchema);
        var constraintName = foreignKey.GetConstraintName(
            storeObjectIdentifier,
            principalStoreObjectIdentifier);

        if (constraintName == null) return;

        var columnNames = foreignKey.Properties
            .Select(p => p.GetColumnName(storeObjectIdentifier))
            .Where(n => n != null)
            .Cast<string>()
            .ToArray();

        var linkedColumnNames = foreignKey.PrincipalKey.Properties
            .Select(p => p.GetColumnName(principalStoreObjectIdentifier))
            .Where(n => n != null)
            .Cast<string>()
            .ToArray();

        if (columnNames.Length == 0 || linkedColumnNames.Length == 0) return;

        var fk = table.AddForeignKey(constraintName, principalIdentifier, columnNames, linkedColumnNames);
        fk.DeleteAction = mapDeleteBehavior(foreignKey.DeleteBehavior);
    }

    private static void mapColumn(IProperty property, StoreObjectIdentifier storeObjectIdentifier,
        HashSet<string> primaryKeyPropertyNames, ITable table)
    {
        var columnName = property.GetColumnName(storeObjectIdentifier);
        if (columnName == null) return;

        var columnType = property.GetColumnType(storeObjectIdentifier);
        var isPrimaryKey = primaryKeyPropertyNames.Contains(property.Name);

        ITableColumn column;
        if (columnType != null)
        {
            column = isPrimaryKey
                ? table.AddPrimaryKeyColumn(columnName, columnType)
                : table.AddColumn(columnName, columnType);
        }
        else
        {
            column = isPrimaryKey
                ? table.AddPrimaryKeyColumn(columnName, property.ClrType)
                : table.AddColumn(columnName, property.ClrType);
        }

        column.AllowNulls = property.IsNullable;

        var defaultValueSql = property.GetDefaultValueSql(storeObjectIdentifier);
        if (defaultValueSql != null)
        {
            column.DefaultExpression = defaultValueSql;
        }
    }

    private static CascadeAction mapDeleteBehavior(DeleteBehavior deleteBehavior)
    {
        return deleteBehavior switch
        {
            DeleteBehavior.Cascade => CascadeAction.Cascade,
            DeleteBehavior.SetNull => CascadeAction.SetNull,
            DeleteBehavior.Restrict => CascadeAction.Restrict,
            DeleteBehavior.NoAction => CascadeAction.NoAction,
            DeleteBehavior.ClientSetNull => CascadeAction.SetNull,
            DeleteBehavior.ClientCascade => CascadeAction.Cascade,
            DeleteBehavior.ClientNoAction => CascadeAction.NoAction,
            _ => CascadeAction.NoAction
        };
    }
}
