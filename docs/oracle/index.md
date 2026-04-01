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
| `Guid` | `RAW(16)` |
| `byte[]` | `BLOB` |

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
