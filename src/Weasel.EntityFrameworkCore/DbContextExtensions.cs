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
    /// <para>
    /// Reflects on the runtime type of each <c>IDbContextOptionsExtension</c> looking for
    /// a <c>DataSource</c> property — the canonical extension carrying the data source isn't
    /// uniformly typed across EF Core provider packages. Suppressed locally; AOT consumers
    /// should rely on EF Core's typed configuration APIs instead of this reflective fallback.
    /// weasel#263.
    /// </para>
    /// </summary>
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2075",
        Justification = "Reflective lookup of the DataSource property on an arbitrary IDbContextOptionsExtension; EF Core upstream is not AOT-ready (dotnet/efcore#29761). weasel#263.")]
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
            // Exclude owned entity types that live inside their owner's table
            // (table splitting / ToJson — https://github.com/JasperFx/weasel/issues/234),
            // but keep owned types mapped to their own table via OwnsOne/OwnsMany
            // ToTable — those are real tables that need migrations.
            .Where(e => !e.IsOwned() || ownedTypeHasItsOwnTable(e))
            // For TPH, multiple entity types share a table. Only keep root types
            // (those without a base type mapped to the same table) to avoid duplicates.
            .GroupBy(e => (e.GetTableName(), e.GetSchema()))
            .Select(g => g.First(e => e.BaseType == null || e.GetTableName() != e.BaseType.GetTableName()))
            .ToList();

        return TopologicalSortByForeignKeys(entityTypes);
    }

    /// <summary>
    ///     True for a foreign key that links two entity types mapped to the SAME
    ///     table over the table's primary key — e.g. a table-split owned type's
    ///     ownership FK back to its owner. Such an FK would reference the row
    ///     itself; EF Core migrations emit no constraint for it. Genuine
    ///     self-referencing FKs (e.g. ParentId) use non-PK columns and are kept.
    /// </summary>
    private static bool isRowInternalForeignKey(IForeignKey foreignKey)
    {
        var dependent = foreignKey.DeclaringEntityType;
        var principal = foreignKey.PrincipalEntityType;

        if (dependent.GetTableName() != principal.GetTableName()
            || dependent.GetSchema() != principal.GetSchema())
        {
            return false;
        }

        var primaryKeyProperties = dependent.FindPrimaryKey()?.Properties.Select(p => p.Name);
        return primaryKeyProperties != null
               && foreignKey.Properties.Select(p => p.Name).SequenceEqual(primaryKeyProperties);
    }

    private static bool ownedTypeHasItsOwnTable(IEntityType entityType)
    {
        if (entityType.IsMappedToJson())
        {
            return false;
        }

        var owner = entityType.FindOwnership()?.PrincipalEntityType;
        if (owner == null)
        {
            return true;
        }

        return entityType.GetTableName() != owner.GetTableName()
               || entityType.GetSchema() != owner.GetSchema();
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

        // EF Core migrations emit quoted, case-sensitive identifiers ("BlogId"),
        // so the Weasel table must reproduce the exact casing. Without this,
        // case-folding providers (PostgreSQL) would create lowercase columns that
        // EF's own quoted SQL cannot find.
        table.PreserveIdentifierCase = true;

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

        // Owned entity types table-split into this table (OwnsOne without ToTable)
        // contribute their columns too. Walk transitively so nested owned types
        // (an owned type of an owned type) are picked up as well.
        bool addedOwned;
        do
        {
            addedOwned = false;
            foreach (var candidate in entityType.Model.GetEntityTypes())
            {
                if (allEntityTypes.Contains(candidate)) continue;
                if (!candidate.IsOwned() || candidate.IsMappedToJson()) continue;
                if (candidate.GetTableName() != tableName || candidate.GetSchema() != efSchema) continue;

                var owner = candidate.FindOwnership()?.PrincipalEntityType;
                if (owner != null && allEntityTypes.Contains(owner))
                {
                    allEntityTypes.Add(candidate);
                    addedOwned = true;
                }
            }
        } while (addedOwned);

        // Add columns from all entity types in the hierarchy
        var addedColumns = new HashSet<string>();
        foreach (var et in allEntityTypes)
        {
            foreach (var property in et.GetProperties())
            {
                var columnName = property.GetColumnName(storeObjectIdentifier);

                // Skip properties EF maps to an engine system column (e.g. Npgsql
                // IsRowVersion() -> PostgreSQL's implicit "xmin"); emitting them as
                // real columns fails with "42701 column name conflicts with a
                // system column name" (weasel#290).
                if (columnName != null && !migrator.IsSystemColumn(columnName) && addedColumns.Add(columnName))
                {
                    mapColumn(property, storeObjectIdentifier, primaryKeyPropertyNames, table);
                }
            }

            // Add JSON columns from owned entities mapped via OwnsOne().ToJson()
            mapJsonColumns(et, addedColumns, table);

#if NET10_0_OR_GREATER
            // Add JSON columns from complex properties / collections mapped via
            // ComplexProperty(...).ToJson() / ComplexCollection(...).ToJson()
            // (EF Core 10+; weasel#291).
            mapComplexJsonColumns(et, addedColumns, table);
#endif
        }

        // Set primary key constraint name from EF Core metadata with its exact
        // casing — Weasel emits it quoted and compares PK names case-insensitively,
        // so the created constraint matches what EF Core's own migrations create.
        if (primaryKey != null)
        {
            var pkName = primaryKey.GetName(storeObjectIdentifier);
            if (pkName != null)
            {
                table.PrimaryKeyName = pkName;
            }
        }

        // Add foreign keys from all entity types in the hierarchy
        var addedForeignKeys = new HashSet<string>();
        foreach (var et in allEntityTypes)
        {
            foreach (var foreignKey in et.GetForeignKeys())
            {
                // A row-internal linking FK (table-split owned type -> owner over the
                // shared PK) points at the row itself; EF Core migrations emit no
                // constraint for it, so neither does Weasel.
                if (isRowInternalForeignKey(foreignKey)) continue;

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

        // Add indexes from all entity types in the hierarchy. This includes the
        // indexes EF Core creates by convention for every foreign key (IX_*) —
        // without them, Weasel's delta detection would see EF's indexes as
        // unknown extras and emit DROP INDEX statements against them.
        var addedIndexes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var et in allEntityTypes)
        {
            mapIndexes(et, storeObjectIdentifier, addedIndexes, table);
        }

        return table;
    }

    private static void mapIndexes(IEntityType entityType, StoreObjectIdentifier storeObjectIdentifier,
        HashSet<string> addedIndexes, ITable table)
    {
        foreach (var index in entityType.GetIndexes())
        {
            var name = index.GetDatabaseName(storeObjectIdentifier);
            if (name == null || !addedIndexes.Add(name)) continue;

            var columnNames = index.Properties
                .Select(p => p.GetColumnName(storeObjectIdentifier))
                .Where(c => c != null)
                .Cast<string>()
                .ToArray();
            if (columnNames.Length != index.Properties.Count) continue;

            var tableIndex = table.AddIndex(name, columnNames, index.IsUnique);

            var filter = index.GetFilter(storeObjectIdentifier);
            if (filter != null)
            {
                tableIndex.Predicate = filter;
            }

            var includeColumns = findIncludeColumns(index, entityType, storeObjectIdentifier);
            if (includeColumns is { Length: > 0 })
            {
                tableIndex.IncludeColumns = includeColumns;
            }
        }

        // Alternate keys (HasAlternateKey) become UNIQUE constraints in EF Core
        // migrations; the database implements those with a unique index, which is
        // how Weasel models them.
        foreach (var key in entityType.GetKeys())
        {
            if (key.IsPrimaryKey()) continue;

            var name = key.GetName(storeObjectIdentifier);
            if (name == null || !addedIndexes.Add(name)) continue;

            var columnNames = key.Properties
                .Select(p => p.GetColumnName(storeObjectIdentifier))
                .Where(c => c != null)
                .Cast<string>()
                .ToArray();
            if (columnNames.Length != key.Properties.Count) continue;

            table.AddIndex(name, columnNames, isUnique: true);
        }
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

        // IsColumnNullable (not IsNullable) accounts for the relational mapping:
        // e.g. in a TPH hierarchy, a required property of a derived type still maps
        // to a nullable column because rows of sibling types have no value for it.
        column.AllowNulls = property.IsColumnNullable(storeObjectIdentifier);

        column.IsAutoNumber = isDatabaseGeneratedIdentity(property, storeObjectIdentifier);

        var defaultValueSql = property.GetDefaultValueSql(storeObjectIdentifier);
        if (defaultValueSql != null)
        {
            column.DefaultExpression = defaultValueSql;
        }
        else if (property.TryGetDefaultValue(storeObjectIdentifier, out var defaultValue) && defaultValue != null)
        {
            // HasDefaultValue(...) stores a literal object rather than SQL; render
            // it with the provider's own type mapping so the literal matches what
            // EF Core migrations would emit (N'...' on SQL Server, TRUE on
            // PostgreSQL, converted values for enum/value-converter properties...)
            var typeMapping = property.FindRelationalTypeMapping(storeObjectIdentifier)
                              ?? property.FindRelationalTypeMapping();
            if (typeMapping != null)
            {
                column.DefaultExpression = typeMapping.GenerateSqlLiteral(defaultValue);
            }
        }
    }

    /// <summary>
    ///     True when EF Core's model expects the database itself to generate the
    ///     column value via an identity / serial / auto-increment column. Detected
    ///     through the provider's ValueGenerationStrategy annotation (property
    ///     level, falling back to the model-wide default) rather than the typed
    ///     provider extension methods, since this assembly only references
    ///     EF Core's relational layer. Sequence-based strategies (HiLo, NEXT VALUE
    ///     FOR defaults) are excluded — those columns are plain columns.
    /// </summary>
    private static bool isDatabaseGeneratedIdentity(IProperty property, in StoreObjectIdentifier storeObjectIdentifier)
    {
        if (property.ValueGenerated != ValueGenerated.OnAdd)
        {
            return false;
        }

        // A configured default (HasDefaultValue/HasDefaultValueSql) IS the
        // OnAdd store generation — not an identity column.
        if (property.GetDefaultValueSql() != null || property.TryGetDefaultValue(out _))
        {
            return false;
        }

        // A key that is also an FK declared by a type mapped to THIS table
        // (TPT/TPC linking key, owned type sharing the owner's key, 1:1
        // shared-PK) takes its value from the principal row, so the column is
        // not identity HERE. The same property CAN still be identity in the
        // principal's own table (the TPT base), so the check is per store object.
        foreach (var foreignKey in property.GetContainingForeignKeys())
        {
            var declaring = foreignKey.DeclaringEntityType;
            if (declaring.GetTableName() == storeObjectIdentifier.Name
                && declaring.GetSchema() == storeObjectIdentifier.Schema)
            {
                return false;
            }
        }

        // Identity columns only exist for integral types
        var clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;
        if (clrType != typeof(int) && clrType != typeof(long) && clrType != typeof(short) && clrType != typeof(byte))
        {
            return false;
        }

        var strategy = findValueGenerationStrategy(property.GetAnnotations())
                       ?? findValueGenerationStrategy(property.DeclaringType.Model.GetAnnotations());

        return strategy is "IdentityColumn" // SQL Server, MySQL (Pomelo), Oracle
            or "IdentityByDefaultColumn" // Npgsql
            or "IdentityAlwaysColumn" // Npgsql
            or "SerialColumn"; // Npgsql legacy UseSerialColumns
    }

    /// <summary>
    ///     Resolve the covering-index INCLUDE columns from the provider's index
    ///     annotation ("SqlServer:Include" / "Npgsql:IndexInclude") — the
    ///     annotation stores property names, which are translated to column names.
    /// </summary>
    private static string[]? findIncludeColumns(IIndex index, IEntityType entityType,
        StoreObjectIdentifier storeObjectIdentifier)
    {
        foreach (var annotation in index.GetAnnotations())
        {
            if (!annotation.Name.EndsWith(":Include", StringComparison.Ordinal) &&
                !annotation.Name.EndsWith(":IndexInclude", StringComparison.Ordinal))
            {
                continue;
            }

            if (annotation.Value is not IReadOnlyList<string> propertyNames) continue;

            return propertyNames
                .Select(p => entityType.FindProperty(p)?.GetColumnName(storeObjectIdentifier))
                .Where(c => c != null)
                .Cast<string>()
                .ToArray();
        }

        return null;
    }

    private static string? findValueGenerationStrategy(IEnumerable<IAnnotation> annotations)
    {
        foreach (var annotation in annotations)
        {
            if (annotation.Name.EndsWith(":ValueGenerationStrategy", StringComparison.Ordinal))
            {
                return annotation.Value?.ToString();
            }
        }

        return null;
    }

    private static void mapJsonColumns(IEntityType entityType, HashSet<string> addedColumns, ITable table)
    {
        foreach (var navigation in entityType.GetNavigations())
        {
            var targetType = navigation.TargetEntityType;
            if (!targetType.IsMappedToJson()) continue;

            // EF Core 10 moved GetContainerColumnName/GetContainerColumnType from
            // RelationalEntityTypeExtensions to RelationalTypeBaseExtensions
#if NET10_0_OR_GREATER
            var columnName = targetType.GetContainerColumnName();
            if (columnName == null || !addedColumns.Add(columnName)) continue;

            var columnType = targetType.GetContainerColumnType() ?? "jsonb";
#else
            var columnName = RelationalEntityTypeExtensions.GetContainerColumnName(targetType);
            if (columnName == null || !addedColumns.Add(columnName)) continue;

            var columnType = RelationalEntityTypeExtensions.GetContainerColumnType(targetType) ?? "jsonb";
#endif
            var column = table.AddColumn(columnName, columnType);
            column.AllowNulls = !navigation.ForeignKey.IsRequired;
        }
    }

#if NET10_0_OR_GREATER
    /// <summary>
    ///     Map the JSON container columns produced by EF Core 10
    ///     <c>ComplexProperty(...).ToJson()</c> / <c>ComplexCollection(...).ToJson()</c>.
    ///     Unlike <c>OwnsOne(...).ToJson()</c> (handled in <see cref="mapJsonColumns" />
    ///     via navigations to a JSON-mapped entity type), complex properties are not
    ///     navigations and carry no separate entity type — their JSON container lives
    ///     on the <see cref="IComplexType" />. Both the single
    ///     (<c>ComplexProperty</c>) and collection (<c>ComplexCollection</c>) shapes
    ///     serialize into a single container column, so each top-level JSON-mapped
    ///     complex property contributes exactly one column (weasel#291).
    /// </summary>
    private static void mapComplexJsonColumns(IEntityType entityType, HashSet<string> addedColumns, ITable table)
    {
        foreach (var complexProperty in entityType.GetComplexProperties())
        {
            var complexType = complexProperty.ComplexType;
            if (!complexType.IsMappedToJson()) continue;

            var columnName = complexType.GetContainerColumnName();
            if (columnName == null || !addedColumns.Add(columnName)) continue;

            var columnType = complexType.GetContainerColumnType() ?? "jsonb";
            var column = table.AddColumn(columnName, columnType);
            column.AllowNulls = complexProperty.IsNullable;
        }
    }
#endif

    /// <summary>
    ///     Translate EF Core's DeleteBehavior into the ON DELETE action EF Core's
    ///     own migrations would emit. The Client* behaviors are enforced purely
    ///     client-side by EF change tracking — EF migrations emit no ON DELETE
    ///     clause for them (ClientSetNull is the DEFAULT for optional
    ///     relationships), so they must map to NoAction here, not to their
    ///     database-enforced cousins.
    /// </summary>
    private static CascadeAction mapDeleteBehavior(DeleteBehavior deleteBehavior)
    {
        return deleteBehavior switch
        {
            DeleteBehavior.Cascade => CascadeAction.Cascade,
            DeleteBehavior.SetNull => CascadeAction.SetNull,
            DeleteBehavior.Restrict => CascadeAction.Restrict,
            _ => CascadeAction.NoAction
        };
    }
}
