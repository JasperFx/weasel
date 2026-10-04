# Firebird Overview

Weasel.Firebird provides schema management and migration support for Firebird 3, 4 and 5 using the `FirebirdSql.Data.FirebirdClient` driver.

## Installation

```bash
dotnet add package Weasel.Firebird
```

## Key Components

- **FirebirdProvider** -- Singleton at `FirebirdProvider.Instance` that handles type mappings and identifier parsing. The default schema is `PUBLIC`; see [No schemas before Firebird 6](#no-schemas-before-firebird-6).
- **FirebirdMigrator** -- Generates DDL as isql scripts and executes schema migrations one statement at a time.
- **FirebirdScript** -- Writes and splits those scripts: `SET TERM` blocks, and a `COMMIT;` after every statement.

## Supported Schema Objects

| Object | Class | Namespace |
|--------|-------|-----------|
| [Tables](/firebird/tables) | `Table` | `Weasel.Firebird.Tables` |
| [Sequences](/firebird/sequences) | `Sequence` | `Weasel.Firebird` |
| [Views](/firebird/views) | `View` | `Weasel.Firebird.Views` |
| [Functions](/firebird/functions) | `Function` | `Weasel.Firebird.Functions` |
| [Stored procedures](/firebird/procedures) | `StoredProcedure` | `Weasel.Firebird.Procedures` |
| [Triggers](/firebird/triggers) | `Trigger` | `Weasel.Firebird.Triggers` |

Tables also carry [computed columns](/firebird/tables#computed-columns) (`COMPUTED BY`, virtual only) and identity
columns.

## Connection String

Weasel.Firebird uses standard `FirebirdSql.Data.FirebirdClient` connection strings:

<!-- snippet: sample_firebird_connection_string -->
<a id='snippet-sample_firebird_connection_string'></a>
```cs
var connectionString =
    "DataSource=localhost;Port=3050;Database=/var/lib/firebird/data/mydb.fdb;User=SYSDBA;Password=YourPassword;Charset=UTF8";
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdSamples.cs#L14-L17' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_connection_string' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

| Setting | Why |
|---|---|
| `Database` | A path on the server, absolute: a relative one is not resolved against the data directory. An alias from `databases.conf` also works |
| `Charset=UTF8` | The connection's character set; also the default of a database `EnsureDatabaseExistsAsync` creates |

## Type Mappings

| .NET Type | Firebird Type |
|-----------|---------------|
| `string` | `VARCHAR(255)` |
| `bool` | `BOOLEAN` |
| `byte`, `short` | `SMALLINT` |
| `int`, enums | `INTEGER` |
| `long` | `BIGINT` |
| `decimal` | `NUMERIC(18,4)` |
| `double` | `DOUBLE PRECISION` |
| `float` | `FLOAT` |
| `DateTime` | `TIMESTAMP` |
| `DateOnly` | `DATE` |
| `TimeOnly`, `TimeSpan` | `TIME` |
| `DateTimeOffset` | `TIMESTAMP WITH TIME ZONE` (Firebird 4+) |
| `Guid` | `CHAR(16) CHARACTER SET OCTETS` |
| `byte[]` | `BLOB SUB_TYPE BINARY` |
| anything else (JSON) | `BLOB SUB_TYPE TEXT` |

## Creating a Migrator

<!-- snippet: sample_firebird_create_migrator -->
<a id='snippet-sample_firebird_create_migrator'></a>
```cs
var migrator = new FirebirdMigrator();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdSamples.cs#L24-L26' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_create_migrator' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

| Property | Default | Meaning |
|---|---|---|
| `MaxIdentifierLength` | `31` | Longest name Weasel writes. See [Identifiers](#identifiers) |
| `LockTimeout` | 10 seconds | How long one DDL statement waits for another transaction's lock |
| `MaxGuardedStatementAttempts` | `5` | Runs of a statement that keeps losing a catalog race before the conflict is reported |
| `NewDatabasePageSize` | `16384` | Page size of a database `EnsureDatabaseExistsAsync` creates. See [Page size](#page-size) |

The migrator can ensure the target database exists:

<!-- snippet: sample_firebird_ensure_database_exists -->
<a id='snippet-sample_firebird_ensure_database_exists'></a>
```cs
await using var conn = new FbConnection(connectionString);
await migrator.EnsureDatabaseExistsAsync(conn);
// Opens mydb.fdb, and creates it only when the file is missing
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdSamples.cs#L35-L39' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_ensure_database_exists' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The database is opened first and created only when its file is missing, so a user that may connect but not create
databases never reaches the create. Creating needs `SYSDBA` or a user granted `CREATE DATABASE`. A create that loses
a race to another process counts as success.

## No schemas before Firebird 6

Firebird 3, 4 and 5 have no schemas. Every object lives in the pseudo-schema `PUBLIC` -- the name Firebird 6 gives
its default schema -- and `PUBLIC` is never written into DDL.

| You write | DDL targets |
|---|---|
| `new Table("users")` | `users` |
| `new Table("PUBLIC.users")` | `users` |
| `new Table("sales.users")` | nothing: `NotSupportedException` |

Any other schema is refused when the DDL is rendered, before the first statement runs. `DropSchemaAsync` empties
the database rather than dropping it, because the connection doing the work is attached to it.

## Identifiers

| | Firebird |
|---|---|
| Delimiter | `"NAME"` |
| Unquoted names fold to | upper case |
| Quoted when | not a regular identifier (an ASCII letter, then letters, digits, `_` or `$`), or a reserved word in Firebird 3, 4 or 5 |
| Rejected | `"` and `^`, beside the [shared rules](/core/identifiers#what-validation-rejects) |
| Longest name | 31 bytes, Firebird 3's limit; 63 characters with `MaxIdentifierLength = 63` |
| A name over the limit | refused, never truncated |

Firebird refuses an over-long name rather than truncating it, so Weasel does too -- for columns and constraint names
as well as objects. Set the longer limit only for a database no Firebird 3 server will open:

<!-- snippet: sample_firebird_identifier_length -->
<a id='snippet-sample_firebird_identifier_length'></a>
```cs
// Only for a database that no Firebird 3 server will open
var migrator = new FirebirdMigrator { MaxIdentifierLength = 63 };
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/FirebirdSamples.cs#L44-L47' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_firebird_identifier_length' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Derived names count too. The default primary key name is `pk_{table}`, so a table name over 28 bytes needs
`PrimaryKeyName` on Firebird 3; the refusal says so. Constraint and index names share one namespace across the
whole database.

## How a migration runs

Everything is rendered first, so a refusal -- a schema, a mixed-direction index, a name over the limit, a key over 16
columns -- fires before any DDL runs. A name longer than the server's catalog holds is refused before introspection
binds it, too. Then each statement runs in a transaction of its own and commits alone:

| | Weasel.Firebird |
|---|---|
| Transaction | one per statement, `WAIT` with `LockTimeout`, rolled back if it fails |
| Tables, columns, keys, indexes, sequences | guarded: created only while missing |
| Views, functions, procedures, triggers | `CREATE OR ALTER`: altered in place, never dropped first |
| Two appliers racing | both succeed: a statement that loses a catalog race is run again, up to `MaxGuardedStatementAttempts` |
| Global lock | none, as on Oracle, MySQL and SQLite |
| Rollback | last delta first |

Concurrent appliers are safe without a lock because a re-run is harmless: the guard makes it a no-op once another
applier has created the object, and `CREATE OR ALTER` leaves the same object however often it runs. A statement is
run again only after a race -- a catalog conflict, a lock conflict or a lock timeout -- never after an ordinary
failure, which would only fail again. The one exception is a hand-written index over 16 columns: Firebird fails it with
the same error as the loser of an index race, so it runs `MaxGuardedStatementAttempts` times before it surfaces.

A rollback undoes the last delta first. Firebird refuses to drop a table, view or procedure that a view, procedure or
trigger still uses, so undoing in the order of the apply could not drop a table before the view over it.

### Beside a running application

FirebirdClient's default is `NO WAIT`, under which DDL beside any uncommitted write fails at once. Under `WAIT`, DDL
waits for another transaction's uncommitted writes to commit, up to `LockTimeout`, and `DROP TABLE` needs the table
idle:

| Change | Another attachment reading the table | Another attachment with uncommitted writes to it |
|---|---|---|
| Nullable `ADD` | runs | runs |
| `ADD … NOT NULL`, `ALTER … TYPE`, `CREATE INDEX`, `ADD CONSTRAINT` | runs | waits for the commit, up to `LockTimeout` |
| `DROP TABLE` | times out while it holds a transaction over the table | same |

Stop the application before a migration drops a table.

::: warning A nullable column added with a default leaves existing rows NULL
Firebird fills existing rows only for `ADD … DEFAULT … NOT NULL`. A nullable column's default applies to new rows.
:::

## Page size

A key over long `VARCHAR` columns in a UTF8 database needs 16 KB pages:

| Page size | Longest indexable UTF8 `VARCHAR` |
|---|---|
| 4096 | 253 |
| 8192 | 509 |
| 16384 | 1021 |

`EnsureDatabaseExistsAsync` creates a database with `NewDatabasePageSize`, 16384. A database created elsewhere --
by the Docker image's `FIREBIRD_DATABASE`, for one -- has 8192 pages.

## Scripts

`db-patch` and `WriteMigrationFileAsync` write isql scripts, which run unchanged with `isql -q -i`:

- PSQL blocks are wrapped in `SET TERM ^ ;` … `SET TERM ; ^`.
- Every statement is followed by `COMMIT;`, as each is committed alone when a migration is applied. Without it, a
  guarded block followed by plain DDL fails at isql's commit, and the guarded object is silently lost.
- A PSQL body with a `^` outside a literal or comment is refused when it is written.
