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
    /// For TPH (Table Per Hierarchy) hierarchies where multiple entity types share
    /// the same table, only the root entity type is returned to avoid duplicate table definitions.
    /// Results are topologically sorted by foreign key dependencies so that referenced tables
    /// are created before the tables that reference them.
    /// </summary>
    public static IReadOnlyList<IEntityType> GetEntityTypesForMigration(DbContext context)
    {
        // The design-time model has full annotation access including IsTableExcludedFromMigrations.
        // The default read-optimized model throws when accessing migration-related annotations.
        var designTimeModel = context.GetService<IDesignTimeModel>();
        var model = designTimeModel?.Model ?? context.Model;

        var entityTypes = model.GetEntityTypes()
            .Where(e => !e.IsTableExcludedFromMigrations())
            .Where(e => e.GetTableName() != null)
            // For TPH, multiple entity types share a table. Only keep root types
            // (those without a base type mapped to the same table) to avoid duplicates.
            .GroupBy(e => (e.GetTableName(), e.GetSchema()))
            .Select(g => g.First(e => e.BaseType == null || e.GetTableName() != e.BaseType.GetTableName()))
            .ToList();

        return TopologicalSortByForeignKeys(entityTypes);
    }

    /// <summary>
    /// Topologically sorts entity types so that entities referenced by foreign keys
    /// appear before the entities that reference them. This ensures DDL statements
    /// create referenced tables before referencing tables.
    /// Falls back to the original order if a cycle is detected.
    /// </summary>
    internal static IReadOnlyList<IEntityType> TopologicalSortByForeignKeys(List<IEntityType> entityTypes)
    {
        if (entityTypes.Count <= 1) return entityTypes;

        // Build a lookup from (TableName, Schema) to entity type for resolving FK targets
        var tableToEntity = new Dictionary<(string? tableName, string? schema), IEntityType>();
        foreach (var et in entityTypes)
        {
            var key = (et.GetTableName(), et.GetSchema());
            tableToEntity.TryAdd(key, et);
        }

        // Build adjacency: for each entity, find which other entities it depends on (via FKs)
        // Use HashSet to avoid duplicate edges from TPH derived types inheriting the same FK
        var dependencies = new Dictionary<IEntityType, HashSet<IEntityType>>();
        foreach (var et in entityTypes)
        {
            var deps = new HashSet<IEntityType>();
            foreach (var fk in et.GetForeignKeys())
            {
                var principalKey = (fk.PrincipalEntityType.GetTableName(), fk.PrincipalEntityType.GetSchema());
                if (tableToEntity.TryGetValue(principalKey, out var principalEntity) && principalEntity != et)
                {
                    deps.Add(principalEntity);
                }
            }

            // Also check derived types in TPH hierarchies for FKs
            foreach (var derived in et.GetDerivedTypesInclusive())
            {
                if (derived == et) continue;
                if (derived.GetTableName() != et.GetTableName() || derived.GetSchema() != et.GetSchema()) continue;

                foreach (var fk in derived.GetForeignKeys())
                {
                    var principalKey = (fk.PrincipalEntityType.GetTableName(), fk.PrincipalEntityType.GetSchema());
                    if (tableToEntity.TryGetValue(principalKey, out var principalEntity) && principalEntity != et)
                    {
                        deps.Add(principalEntity);
                    }
                }
            }

            dependencies[et] = deps;
        }

        // Kahn's algorithm for topological sort
        var inDegree = entityTypes.ToDictionary(e => e, _ => 0);
        foreach (var et in entityTypes)
        {
            foreach (var dep in dependencies[et])
            {
                if (inDegree.ContainsKey(dep))
                {
                    inDegree[et]++;
                }
            }
        }

        var queue = new Queue<IEntityType>();
        foreach (var et in entityTypes)
        {
            if (inDegree[et] == 0)
            {
                queue.Enqueue(et);
            }
        }

        var sorted = new List<IEntityType>(entityTypes.Count);
        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            sorted.Add(current);

            // Find entities that depend on 'current' and reduce their in-degree
            foreach (var et in entityTypes)
            {
                if (dependencies[et].Contains(current))
                {
                    inDegree[et]--;
                    if (inDegree[et] == 0)
                    {
                        queue.Enqueue(et);
                    }
                }
            }
        }

        // If cycle detected, fall back to original order
        return sorted.Count == entityTypes.Count ? sorted : entityTypes;
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

        // Collect all entity types sharing this table (for TPH hierarchies).
        // This includes the root entity type and all derived types mapped to the same table.
        var allEntityTypes = entityType.GetDerivedTypesInclusive()
            .Where(e => e.GetTableName() == tableName && e.GetSchema() == efSchema)
            .ToList();

        // Add columns from all entity types in the hierarchy
        var addedColumns = new HashSet<string>();
        foreach (var et in allEntityTypes)
        {
            foreach (var property in et.GetProperties())
            {
                var columnName = property.GetColumnName(storeObjectIdentifier);
                if (columnName != null && addedColumns.Add(columnName))
                {
                    mapColumn(property, storeObjectIdentifier, primaryKeyPropertyNames, table);
                }
            }

            // Add JSON columns from owned entities mapped via OwnsOne().ToJson()
            mapJsonColumns(et, addedColumns, table);
        }

        // Set primary key constraint name from EF Core metadata.
        // Normalize to lowercase to match PostgreSQL's convention of folding unquoted
        // identifiers to lowercase. This prevents spurious RENAME CONSTRAINT migrations
        // when EF Core generates PascalCase names (e.g., "PK_items") but PostgreSQL
        // stores them as lowercase ("pk_items").
        if (primaryKey != null)
        {
            var pkName = primaryKey.GetName(storeObjectIdentifier);
            if (pkName != null)
            {
                table.PrimaryKeyName = pkName.ToLowerInvariant();
            }
        }

        // Add foreign keys from all entity types in the hierarchy
        var addedForeignKeys = new HashSet<string>();
        foreach (var et in allEntityTypes)
        {
            foreach (var foreignKey in et.GetForeignKeys())
            {
                var constraintName = foreignKey.GetConstraintName(
                    storeObjectIdentifier,
                    StoreObjectIdentifier.Table(
                        foreignKey.PrincipalEntityType.GetTableName()!,
                        foreignKey.PrincipalEntityType.GetSchema()));
                if (constraintName != null && addedForeignKeys.Add(constraintName))
                {
                    mapForeignKey(migrator, foreignKey, storeObjectIdentifier, table);
                }
            }
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

        var fk = table.AddForeignKey(constraintName.ToLowerInvariant(), principalIdentifier, columnNames, linkedColumnNames);
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

    private static void mapJsonColumns(IEntityType entityType, HashSet<string> addedColumns, ITable table)
    {
        foreach (var navigation in entityType.GetNavigations())
        {
            var targetType = navigation.TargetEntityType;
            if (!targetType.IsMappedToJson()) continue;

            var columnName = targetType.GetContainerColumnName();
            if (columnName == null || !addedColumns.Add(columnName)) continue;

            var columnType = targetType.GetContainerColumnType() ?? "jsonb";
            var column = table.AddColumn(columnName, columnType);
            column.AllowNulls = !navigation.ForeignKey.IsRequired;
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
