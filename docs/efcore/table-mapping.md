# Table Mapping

The `MapToTable()` extension method on `Migrator` converts an EF Core `IEntityType` into a Weasel `ITable`. This is the core of the EF Core integration -- it reads EF Core's metadata and produces a fully defined Weasel table object that participates in delta detection and DDL generation.

## What Gets Mapped

| EF Core Metadata | Weasel Table Property |
|---|---|
| Table name and schema | `ITable.Identifier` |
| Column name, type, nullability | `ITableColumn` with type, `AllowNulls` |
| Max length, column type annotations | Column type string |
| Default value SQL | `ITableColumn.DefaultExpression` |
| Primary key and constraint name | Primary key columns + `PrimaryKeyName` |
| Foreign keys with delete behavior | `ITable.AddForeignKey()` with `CascadeAction` |
| Indexes (unique, filtered, composite) | Index definitions |
| JSON columns via `OwnsOne().ToJson()` | Column with `jsonb` type (see [JSON Columns](./json-columns)) |

## Basic Usage

<!-- snippet: sample_efcore_table_mapping_basic -->
<a id='snippet-sample_efcore_table_mapping_basic'></a>
```cs
var migrator = new PostgresqlMigrator(); // or SqlServerMigrator
using var context = dbContext;

foreach (var entityType in DbContextExtensions.GetEntityTypesForMigration(context))
{
    var table = migrator.MapToTable(entityType);
    // table is now a Weasel ITable with full schema definition
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreSamples.cs#L56-L65' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_table_mapping_basic' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Entity Type Filtering

`GetEntityTypesForMigration()` applies several filters before returning entity types:

- **Excluded from migrations** -- Entity types marked with `ExcludeFromMigrations()` are skipped.
- **No table name** -- Entity types without a mapped table (e.g., keyless query types) are skipped.
- **Owned types** -- Entity types configured via `OwnsOne()` or `OwnsMany()` are excluded since they do not have their own tables. Their JSON-mapped properties are handled separately (see [JSON Columns](./json-columns)).

## TPH (Table Per Hierarchy) Handling

When multiple entity types in a TPH hierarchy share the same table, only the root entity type produces a Weasel table. However, columns from all derived types in the hierarchy are included in that table definition. This prevents duplicate table definitions while ensuring all columns (including discriminator-driven columns from derived types) are present.

For example, if `Animal` is the root and `Dog`/`Cat` are derived types sharing the `"animals"` table, `GetEntityTypesForMigration` returns only `Animal`, but the resulting `ITable` includes columns from `Dog` and `Cat` as well.

## Foreign Key Dependencies and Topological Sorting

Entity types are topologically sorted by foreign key relationships using Kahn's algorithm. This ensures that when DDL is generated, referenced tables are created before the tables that reference them. If a circular dependency is detected, the original order is preserved as a fallback.

Foreign keys from TPH derived types are also considered -- if a derived entity in a TPH hierarchy has a foreign key to another table, that dependency is attributed to the root entity type that owns the table.

## Schema Resolution

The table schema is resolved as follows:

1. If the entity type has an explicit schema via `.ToTable("name", "schema")`, that schema is used.
2. Otherwise, the `Migrator.DefaultSchemaName` is used (e.g., `public` for PostgreSQL, `dbo` for SQL Server).

## Constraint Name Normalization

Primary key and foreign key constraint names are normalized to lowercase. This prevents spurious migration diffs when EF Core generates PascalCase names (e.g., `PK_items`) but the database stores them as lowercase (common with PostgreSQL's identifier folding).
