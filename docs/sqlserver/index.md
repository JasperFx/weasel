# SQL Server Overview

Weasel.SqlServer provides schema management and migration support for Microsoft SQL Server databases using the `Microsoft.Data.SqlClient` driver.

## Installation

```bash
dotnet add package Weasel.SqlServer
```

## Key Components

- **SqlServerProvider** -- Singleton at `SqlServerProvider.Instance` that handles type mappings and identifier parsing. The default schema is `dbo`.
- **SqlServerMigrator** -- Generates DDL scripts, executes schema migrations, and manages schema creation/deletion for SQL Server.

## Supported Schema Objects

| Object | Class | Namespace |
|--------|-------|-----------|
| Tables | `Table` | `Weasel.SqlServer.Tables` |
| Stored Procedures | `StoredProcedure` | `Weasel.SqlServer.Procedures` |
| Functions | `Function` | `Weasel.SqlServer.Functions` |
| Sequences | `Sequence` | `Weasel.SqlServer` |
| Table Types | `TableType` | `Weasel.SqlServer.Tables` |

Tables can also be [range or managed-tenant partitioned](/sqlserver/partitioning) via partition functions and schemes.

## Connection String

Weasel.SqlServer uses standard SQL Server connection strings with `Microsoft.Data.SqlClient`:

<!-- snippet: sample_ss_connection_string -->
<a id='snippet-sample_ss_connection_string'></a>
```cs
var connectionString = "Server=localhost;Database=mydb;User Id=sa;Password=YourPassword;TrustServerCertificate=true;";
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L16-L18' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_connection_string' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Creating a Migrator

<!-- snippet: sample_ss_create_migrator -->
<a id='snippet-sample_ss_create_migrator'></a>
```cs
var migrator = new SqlServerMigrator();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L23-L25' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_create_migrator' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The migrator can also ensure the target database exists before running migrations:

<!-- snippet: sample_ss_ensure_database_exists -->
<a id='snippet-sample_ss_ensure_database_exists'></a>
```cs
await using var conn = new SqlConnection(connectionString);
await migrator.EnsureDatabaseExistsAsync(conn);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L33-L36' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_ensure_database_exists' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Schema Management

SQL Server schemas are created automatically when needed during migration. You can also generate schema DDL directly:

<!-- snippet: sample_ss_schema_management -->
<a id='snippet-sample_ss_schema_management'></a>
```cs
var migrator = new SqlServerMigrator();
var writer = new StringWriter();
migrator.WriteSchemaCreationSql(new[] { "myschema" }, writer);
// Generates: IF NOT EXISTS (...) EXEC('CREATE SCHEMA [myschema]');
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqlServerSamples.cs#L41-L46' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ss_schema_management' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Identifiers

SQL Server delimits with `[name]` and escapes an embedded `]` by doubling it. Weasel brackets any
name that is not a regular identifier or that is a reserved word, and leaves ordinary names bare.

Two things changed in 9.25 that are visible from the outside:

- **Names that were previously emitted bare are now bracketed.** Ordinary schemas are
  byte-identical; anything that is not a regular identifier now brackets, because the DDL was
  invalid before.
- **A name you bracketed yourself is honoured, and stored unbracketed in the model.**
  `AddColumn("[Order Date]", …)` names the column `Order Date`, and `TableColumn.Name`,
  `IndexDefinition.Name`, `ForeignKey.Name` and `PrimaryKeyName` all hold the bare name. A name
  that genuinely contains its own brackets can no longer be expressed.

Unlike PostgreSQL, Oracle and SQLite, SQL Server never folds the casing you supply — legacy
schemas are full of PascalCase and lowercasing produced duplicate-column DDL.

See [Identifiers and Quoting](/core/identifiers) for the cross-provider rules and
[Upgrading to 9.25](/release-9-25) for the migration notes.
