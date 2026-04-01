# PostgreSQL Overview

Weasel.Postgresql provides full schema management and migration support for PostgreSQL databases using the Npgsql driver.

## Installation

```bash
dotnet add package Weasel.Postgresql
```

## Key Components

**PostgresqlProvider** is the singleton that handles type mappings between .NET types and PostgreSQL column types. It uses `public` as the default schema.

<!-- snippet: sample_pg_access_provider_singleton -->
<a id='snippet-sample_pg_access_provider_singleton'></a>
```cs
// Access the singleton
var provider = PostgresqlProvider.Instance;

// Map a .NET type to a PostgreSQL type
string dbType = provider.GetDatabaseType(typeof(string), EnumStorage.AsInteger);
// Returns "varchar"
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlIndexSamples.cs#L13-L20' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_access_provider_singleton' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

**PostgresqlMigrator** controls DDL generation rules including formatting, transactional wrapping, and schema creation. It also validates identifier lengths against PostgreSQL's `NAMEDATALEN` limit.

<!-- snippet: sample_pg_configure_migrator -->
<a id='snippet-sample_pg_configure_migrator'></a>
```cs
var migrator = new PostgresqlMigrator
{
    Formatting = SqlFormatting.Pretty,
    IsTransactional = true
};
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlIndexSamples.cs#L25-L31' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_configure_migrator' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

**PostgresqlDatabase** is the abstract base class for building a database with managed schema objects. It requires an `NpgsqlDataSource` for connection management.

<!-- snippet: sample_pg_app_database -->
<a id='snippet-sample_pg_app_database'></a>
```cs
public class AppDatabase : PostgresqlDatabase
{
    public AppDatabase(NpgsqlDataSource dataSource)
        : base(new DefaultMigrationLogger(), AutoCreate.CreateOrUpdate,
               new PostgresqlMigrator(), "app", dataSource)
    {
    }

    public override IFeatureSchema[] BuildFeatureSchemas()
    {
        // Return your feature schemas containing tables, functions, sequences, etc.
        return Array.Empty<IFeatureSchema>();
    }
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlIndexSamples.cs#L34-L49' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_app_database' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Supported Schema Objects

| Object | Class | Description |
|--------|-------|-------------|
| Tables | `Weasel.Postgresql.Tables.Table` | Full DDL with columns, indexes, foreign keys, partitioning |
| Functions | `Weasel.Postgresql.Functions.Function` | PL/pgSQL functions with delta detection |
| Sequences | `Weasel.Postgresql.Sequence` | Auto-incrementing sequences |
| Views | `Weasel.Postgresql.Views.View` | Standard and materialized views |
| Extensions | `Weasel.Postgresql.Extension` | PostgreSQL extensions (e.g., `uuid-ossp`, `postgis`) |

## NpgsqlDataSource Integration

Weasel uses Npgsql's `NpgsqlDataSource` for connection management rather than raw connection strings. This aligns with the modern Npgsql connection pooling model.

<!-- snippet: sample_pg_use_npgsql_datasource -->
<a id='snippet-sample_pg_use_npgsql_datasource'></a>
```cs
var dataSourceBuilder = new NpgsqlDataSourceBuilder("Host=localhost;Database=myapp;");
await using var dataSource = dataSourceBuilder.Build();

var database = new AppDatabase(dataSource);
await database.ApplyAllConfiguredChangesToDatabaseAsync();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlIndexSamples.cs#L53-L59' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_use_npgsql_datasource' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
