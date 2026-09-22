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

## Batch separators and re-runnable scripts

A rendered migration concatenates every object's DDL into one script, and SQL Server requires
`CREATE OR ALTER PROCEDURE` to be the first statement of its batch. So a generated script contains
`GO` lines around each stored procedure:

```sql
IF TYPE_ID(N'gh593.ChildIdList') IS NULL
CREATE TYPE gh593.ChildIdList AS TABLE (ID uniqueidentifier NOT NULL)
GO

CREATE OR ALTER PROCEDURE gh593.uspDeleteChildren
    @IDLIST gh593.ChildIdList READONLY
AS
    DELETE FROM gh593.child WHERE id IN (SELECT ID FROM @IDLIST);
GO
```

`sqlcmd -i` and SQL Server Management Studio both understand `GO`, so a script written out with
`db-patch` or `WriteAllUpdates` runs as it stands. `GO` is not T-SQL, though, so `SqlClient` would
answer `Incorrect syntax near 'GO'` if the text were handed to it whole. Weasel's own executors
therefore split the script first and send one command per batch, with sqlcmd's semantics:

- A separator is a line whose entire content is `GO`, ignoring surrounding whitespace.
- It is case insensitive, so `go` separates too.
- The optional repeat count (`GO 5`) is accepted and ignored. The batch runs once, which is the
  only thing a migration can mean, and nothing in Weasel emits a count.
- String literals and comments are not parsed, exactly as sqlcmd does not parse them. A line
  reading only `GO` inside a literal ends the batch there. Do not author one.

A rendered script also begins with `SET QUOTED_IDENTIFIER ON;`, so it needs no extra flags. sqlcmd
is the one client that leaves that setting off, and SQL Server refuses to create a filtered index,
an index on a computed column or an indexed view while it is off. The header saves you from a
failure that is both quiet and cascading: the batch aborts at the index, every statement after it
in that batch is skipped, and sqlcmd still exits 0. `-b` is still worth passing, because it is what
makes a failed batch set a non-zero exit code:

```bash
sqlcmd -S localhost -d mydb -b -i migration.sql
```

The header is written by the script wrapper, so it appears in files written by
`WriteMigrationFileAsync`, `WriteTemplatedFile` and `ToDatabaseScript`, and not in the DDL the
runtime executor applies. `SqlClient` already defaults `QUOTED_IDENTIFIER` on, and `SET
QUOTED_IDENTIFIER` takes effect at parse time for the batch that contains it and then persists for
the session, so no `GO` is needed after it.

The generated DDL is also re-runnable. Table creation, index creation, foreign key constraints,
table types and sequences each carry their own existence guard (`IF OBJECT_ID(...) IS NULL`,
`IF NOT EXISTS (SELECT 1 FROM sys.indexes ...)`, `IF TYPE_ID(...) IS NULL`), procedures are emitted
as `CREATE OR ALTER`, and index drops use `DROP INDEX IF EXISTS`. Running the same script twice
against the same database is a no-op the second time rather than a failure, which matters because
one unguarded object aborts every statement after it.

`DROP INDEX IF EXISTS` and `CREATE OR ALTER` require **SQL Server 2016 SP1 or later**.

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
