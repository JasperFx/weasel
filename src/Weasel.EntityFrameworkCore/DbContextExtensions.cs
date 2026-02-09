using System.Data.Common;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Weasel.Core;
using Weasel.Core.Migrations;
using ITable = Weasel.Core.ITable;

namespace Weasel.EntityFrameworkCore;

public record DbContextMigration(DbConnection connection, Migrator Migrator, SchemaMigration Migration);

public static class DbContextExtensions
{
    public static async Task<DbContextMigration> CreateMigrationAsync(
        this IServiceProvider services,
        DbContext context,
        CancellationToken cancellation)
    {
        var (conn, migrator) = services.FindMigratorForDbContext(context);

        await using var dbConnection = conn;

        var tables = context
            .Model
            .GetEntityTypes()
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
