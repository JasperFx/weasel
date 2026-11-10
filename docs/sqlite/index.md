# SQLite Overview

Weasel.Sqlite provides schema management and migration support for SQLite databases using the Microsoft.Data.Sqlite driver. It follows the same patterns as other Weasel providers while respecting SQLite's unique characteristics.

## Installation

```bash
dotnet add package Weasel.Sqlite
```

## Key Components

- **SqliteMigrator** -- DDL generation with transaction support
- **SqliteProvider** -- Type mappings and schema introspection
- **SqliteHelper** -- Simplified connection creation with PRAGMA configuration

## Schema Support

SQLite supports two built-in schemas:

| Schema | Description | DDL Prefix |
|--------|-------------|------------|
| `main` | Primary database schema (default) | None |
| `temp` | Temporary objects, connection-scoped | `"temp".` |

Objects in the `main` schema omit the schema prefix in generated DDL. Objects in `temp` or attached database schemas include a quoted prefix.

## Type Mappings

| .NET Type | SQLite Type | Storage Class |
|-----------|-------------|---------------|
| `int`, `long`, `short`, `byte` | INTEGER | INTEGER |
| `bool` | INTEGER | INTEGER |
| `float`, `double`, `decimal` | REAL | REAL |
| `string`, `char` | TEXT | TEXT |
| `DateTime`, `DateTimeOffset` | TEXT | TEXT (ISO8601) |
| `Guid` | TEXT | TEXT |
| `byte[]` | BLOB | BLOB |
| Complex types (JSON) | TEXT | TEXT |

## ALTER TABLE Limitations

SQLite has restricted ALTER TABLE support compared to PostgreSQL or SQL Server:

- **Supported:** ADD COLUMN (nullable or with DEFAULT), DROP COLUMN (3.35+), RENAME COLUMN (3.25+)
- **Not supported:** ADD CONSTRAINT, DROP CONSTRAINT, most ALTER COLUMN changes

When a schema change cannot be applied incrementally, Weasel automatically handles table recreation: create new table, copy data, drop old table, rename.

## Supported Objects

- [Tables](/sqlite/tables) -- Full DDL generation, delta detection, and migration
- [Views](/sqlite/views) -- CREATE/DROP with automatic recreation on changes

## Quick Example

<!-- snippet: sample_sqlite_quick_example -->
<a id='snippet-sample_sqlite_quick_example'></a>
```cs
var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();

var table = new Table("users");
table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
table.AddColumn<string>("name").NotNull();

var migrator = new SqliteMigrator();
var writer = new StringWriter();
table.WriteCreateStatement(migrator, writer);

var cmd = connection.CreateCommand();
cmd.CommandText = writer.ToString();
await cmd.ExecuteNonQueryAsync();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L16-L31' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_quick_example' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Native SQLite bundle

Weasel.Sqlite references `Microsoft.Data.Sqlite` plus `SQLitePCLRaw.bundle_e_sqlite3` on the
SQLitePCLRaw 3.0.x line (currently 3.0.5). `Microsoft.Data.Sqlite` 11 depends on that same bundle and
version, so moving to it needs no change here: Weasel does not swap in `SQLite3MC.PCLRaw` (an encrypted
build, which is a choice for an application to make) and does not cap `Microsoft.Data.Sqlite` below 11.
An application that wants SQLite3MC can reference its provider package itself, since
`SQLite3MC.PCLRaw.provider` is built on SQLitePCLRaw 3.0.x as well.
