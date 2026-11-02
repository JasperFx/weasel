# Oracle Overview

Weasel.Oracle provides schema management and migration support for Oracle databases using the `Oracle.ManagedDataAccess.Core` driver.

## Installation

```bash
dotnet add package Weasel.Oracle
```

## Key Components

- **OracleProvider** -- Singleton at `OracleProvider.Instance` that handles type mappings and identifier parsing. The default schema is `WEASEL`.
- **OracleMigrator** -- Generates DDL scripts and executes schema migrations using PL/SQL blocks.

## Supported Schema Objects

| Object | Class | Namespace |
|--------|-------|-----------|
| Tables | `Table` | `Weasel.Oracle.Tables` |
| Sequences | `Sequence` | `Weasel.Oracle` |

## Connection String

Weasel.Oracle uses standard Oracle connection strings:

<!-- snippet: sample_oracle_connection_string -->
<a id='snippet-sample_oracle_connection_string'></a>
```cs
var connectionString = "User Id=myuser;Password=mypass;Data Source=localhost:1521/XEPDB1;";
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/OracleSamples.cs#L14-L16' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_oracle_connection_string' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Type Mappings

| .NET Type | Oracle Type |
|-----------|-------------|
| `string` | `VARCHAR2(4000)` |
| `int` | `NUMBER(10)` |
| `long` | `NUMBER(19)` |
| `bool` | `NUMBER(1)` |
| `decimal` | `NUMBER` |
| `double` | `BINARY_DOUBLE` |
| `DateTime` | `DATE` |
| `DateTimeOffset` | `TIMESTAMP WITH TIME ZONE` |
| `TimeSpan` | `INTERVAL DAY TO SECOND` |
| `DateOnly` | `DATE` (column only -- see below) |
| `TimeOnly` | `INTERVAL DAY TO SECOND` (column only -- see below) |
| `Guid` | `RAW(16)` |
| `byte[]` | `BLOB` |

### `DateOnly` and `TimeOnly` cannot be bound as parameters

Weasel generates a column type for both, so a schema can declare one -- but ODP.NET has no
conversion for either CLR type, so neither can be used as a parameter value. Measured against
ODP.NET 23.1.0.0, with the `OracleDbType` assigned before the value:

| Binding | Result |
|---|---|
| `DateOnly` as `Date` | `InvalidCastException` -- ODP.NET binds a date through `Convert.ToDateTime`, and `DateOnly` is not `IConvertible` |
| `DateOnly` or `TimeOnly` as `TimeStamp` | `ORA-50028: Invalid parameter binding` |
| `TimeOnly` as `IntervalDS` | `ArgumentException: Value does not fall within the expected range` |
| `TimeOnly` as `Varchar2` | executes, and writes the current culture's `ToString()` -- e.g. `1:45 PM` |

Leaving the parameter untyped is not an alternative either: an untyped `OracleParameter` rejects
the value outright, so there is nothing for the driver to infer from. `ToParameterType` therefore
refuses with a `NotSupportedException` that names the conversion to make instead:

```csharp
// DateOnly against a DATE column
provider.AddNamedParameter(command, "d", theDate.ToDateTime(TimeOnly.MinValue));

// TimeOnly against an INTERVAL DAY TO SECOND column
provider.AddNamedParameter(command, "t", theTime.ToTimeSpan());
```

Note that Entity Framework Core is unaffected: `Oracle.EntityFrameworkCore` applies its own value
converter for both types, so an EF model mapping a `DateOnly` property keeps working.

## Creating a Migrator

<!-- snippet: sample_oracle_create_migrator -->
<a id='snippet-sample_oracle_create_migrator'></a>
```cs
var migrator = new OracleMigrator();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/OracleSamples.cs#L21-L23' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_oracle_create_migrator' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Schemas in Oracle are implemented as database users. The migrator handles this automatically:

<!-- snippet: sample_oracle_ensure_database_exists -->
<a id='snippet-sample_oracle_ensure_database_exists'></a>
```cs
await using var conn = new OracleConnection(connectionString);
await migrator.EnsureDatabaseExistsAsync(conn);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/OracleSamples.cs#L31-L34' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_oracle_ensure_database_exists' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
