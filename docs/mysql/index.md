# MySQL Overview

Weasel.MySql provides schema management and migration support for MySQL databases using the `MySqlConnector` driver.

## Installation

```bash
dotnet add package Weasel.MySql
```

## Key Components

- **MySqlProvider** -- Singleton at `MySqlProvider.Instance` that handles type mappings and identifier parsing. The default schema is `public`.
- **MySqlMigrator** -- Generates DDL scripts and executes schema migrations. Identifiers are quoted with backticks.

## Supported Schema Objects

| Object | Class | Namespace |
|--------|-------|-----------|
| Tables | `Table` | `Weasel.MySql.Tables` |
| Sequences | `Sequence` | `Weasel.MySql` |

## Connection String

Weasel.MySql uses standard MySQL connection strings compatible with `MySqlConnector`:

<!-- snippet: sample_mysql_connection_string -->
<a id='snippet-sample_mysql_connection_string'></a>
```cs
var connectionString = "Server=localhost;Database=mydb;User=root;Password=YourPassword;";
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MySqlSamples.cs#L14-L16' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_mysql_connection_string' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Type Mappings

| .NET Type | MySQL Type |
|-----------|------------|
| `string` | `VARCHAR(255)` |
| `int` | `INT` |
| `long` | `BIGINT` |
| `bool` | `TINYINT(1)` |
| `decimal` | `DECIMAL(18,2)` |
| `double` | `DOUBLE` |
| `DateTime` | `DATETIME` |
| `Guid` | `CHAR(36)` |
| `byte[]` | `BLOB` |

## Creating a Migrator

<!-- snippet: sample_mysql_create_migrator -->
<a id='snippet-sample_mysql_create_migrator'></a>
```cs
var migrator = new MySqlMigrator();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MySqlSamples.cs#L21-L23' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_mysql_create_migrator' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The migrator can ensure the target database exists:

<!-- snippet: sample_mysql_ensure_database_exists -->
<a id='snippet-sample_mysql_ensure_database_exists'></a>
```cs
await using var conn = new MySqlConnection(connectionString);
await migrator.EnsureDatabaseExistsAsync(conn);
// Generates: CREATE DATABASE IF NOT EXISTS `mydb`;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MySqlSamples.cs#L31-L35' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_mysql_ensure_database_exists' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Identifiers

MySQL delimits with backticks and **delimits unconditionally** — it is the one provider for which
"was this name left bare?" is never the right question. An embedded backtick is escaped by
doubling it. A backslash is rejected outright: it is an escape character inside MySQL string
literals unless `NO_BACKSLASH_ESCAPES` is set, so a trailing one would swallow the closing quote
of a literal a name is written into.

9.25 normalizes identifiers on the way into the model, which is visible in two places:

- `Table(DbObjectName)` normalizes to `MySqlObjectName`, so `Table.Identifier.QualifiedName`
  renders as `` `schema`.`name` `` rather than `schema.name`. `Table(string)` already parsed
  through `MySqlProvider` and is unaffected.
- **`table.Identifier` no longer compares equal to a plain `new DbObjectName(schema, name)`** for
  the same table. That inequality was the fix, not the bug — a hand-built identifier never
  matched the catalog, so foreign keys reported drift on every check.

See [Identifiers and Quoting](/core/identifiers) for the cross-provider rules and
[Upgrading to 9.25](/release-9-25) for the migration notes.
