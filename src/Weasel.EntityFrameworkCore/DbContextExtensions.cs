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
        var migrators = services.GetServices<Migrator>().ToList();
        if (!migrators.Any())
        {
            throw new InvalidOperationException($"No {typeof(Migrator).FullNameInCode()} services are registered!");
        }

        await using var conn = context.Database.GetDbConnection();
        var migrator = migrators.FirstOrDefault(x => x.MatchesConnection(conn));
        if (migrator == null)
        {
            throw new InvalidOperationException(
                $"No matching {typeof(Migrator).FullNameInCode()} instances for DbContext {context}. Registered migrators are {migrators.Select(x => x.ToString()).Join(", ")}");
        }

        var tables = context
            .Model
            .GetEntityTypes()
            .Select(x => migrator.MapToTable(x))
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

    public static ITable MapToTable(this Migrator migrator, IEntityType entityType)
    {
        var tableName = entityType.GetTableName();
        var schemaName = entityType.GetSchema() ?? migrator.DefaultSchemaName;

        if (tableName == null)
        {
            throw new InvalidOperationException($"Entity type {entityType.Name} does not have a table name configured.");
        }

        var identifier = migrator.Provider.Parse(schemaName, tableName);
        var table = migrator.CreateTable(identifier);

        var storeObjectIdentifier = StoreObjectIdentifier.Table(tableName, schemaName);

        // Get primary key columns
        var primaryKey = entityType.FindPrimaryKey();
        var primaryKeyPropertyNames = primaryKey?.Properties
            .Select(p => p.Name)
            .ToHashSet() ?? [];

        // Add columns from properties
        foreach (var property in entityType.GetProperties())
        {
            var columnName = property.GetColumnName(storeObjectIdentifier);
            if (columnName == null) continue;

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

        return table;
    }
}
