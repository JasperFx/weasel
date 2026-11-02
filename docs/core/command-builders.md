# Command Builders & Batching

ADO.NET's `DbCommand` API is powerful but verbose. Weasel provides `ICommandBuilder` and `BatchBuilder` to simplify SQL construction with proper parameterization and support for batching multiple commands in a single round trip.

## Overview

Weasel offers two levels of command building:

- **CommandBuilder** -- builds a single `DbCommand` with parameterized SQL, useful for one-off queries.
- **BatchBuilder** -- builds multiple commands within a `DbBatch`, sending them to the database in a single network round trip for better performance.

Each database provider has its own `ICommandBuilder` interface and `BatchBuilder` class because parameter handling differs between engines.

## ICommandBuilder

The provider-specific `ICommandBuilder` interface (defined in both `Weasel.Postgresql` and `Weasel.SqlServer`) exposes methods for building SQL incrementally:

<!-- snippet: sample_ICommandBuilder_interface -->
<a id='snippet-sample_icommandbuilder_interface'></a>
```cs
public interface ICommandBuilder_Sample
{
    string TenantId { get; set; }
    string? LastParameterName { get; }

    void Append(string sql);
    void Append(char character);
    void AppendParameter<T>(T value);
    void AppendWithParameters(string text);
    void StartNewCommand();
    // ... additional members
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/CommandBuilderSamples.cs#L6-L19' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_icommandbuilder_interface' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Key methods:

| Method | Purpose |
|--------|---------|
| `Append(string)` | Append raw SQL text to the current command. |
| `AppendParameter<T>(value)` | Append a parameterized value. Returns the parameter placeholder (e.g., `$1` or `@p0`). |
| `StartNewCommand()` | Begin a new command in the batch. |
| `AppendWithParameters(text)` | Append SQL with `?` placeholders that are replaced by parameter references. |
| `LastParameterName` | The name of the most recently appended parameter. |

## Provider Differences

PostgreSQL and SQL Server handle parameters differently:

| Feature | PostgreSQL | SQL Server |
|---------|-----------|------------|
| Batch type | `NpgsqlBatch` | `SqlBatch` |
| Parameter style | Positional (`$1`, `$2`, `$3`) | Named (`@p0`, `@p1`, `@p2`) |
| Namespace | `Weasel.Postgresql` | `Weasel.SqlServer` |

## PostgreSQL BatchBuilder Example

<!-- snippet: sample_postgresql_batch_builder -->
<a id='snippet-sample_postgresql_batch_builder'></a>
```cs
await using var dataSource = NpgsqlDataSource.Create(connectionString);
await using var batch = dataSource.CreateBatch();
var builder = new Weasel.Postgresql.BatchBuilder(batch);

// First command
builder.Append("INSERT INTO people (name, email) VALUES (");
builder.AppendParameter("Alice");
builder.Append(", ");
builder.AppendParameter("alice@example.com");
builder.Append(")");

// Second command in the same batch
builder.StartNewCommand();
builder.Append("INSERT INTO people (name, email) VALUES (");
builder.AppendParameter("Bob");
builder.Append(", ");
builder.AppendParameter("bob@example.com");
builder.Append(")");

builder.Compile();

// Both inserts execute in a single round trip
await batch.ExecuteNonQueryAsync();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/CommandBuilderSamples.cs#L27-L51' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_postgresql_batch_builder' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The generated SQL for the first command uses positional parameters: `INSERT INTO people (name, email) VALUES ($1, $2)`.

## SQL Server BatchBuilder Example

<!-- snippet: sample_sqlserver_batch_builder -->
<a id='snippet-sample_sqlserver_batch_builder'></a>
```cs
await using var connection = new SqlConnection(connectionString);
await connection.OpenAsync();

await using var batch = new SqlBatch { Connection = connection };
var builder = new Weasel.SqlServer.BatchBuilder(batch);

// First command
builder.Append("INSERT INTO people (name, email) VALUES (");
builder.AppendParameter("Alice");
builder.Append(", ");
builder.AppendParameter("alice@example.com");
builder.Append(")");

// Second command
builder.StartNewCommand();
builder.Append("INSERT INTO people (name, email) VALUES (");
builder.AppendParameter("Bob");
builder.Append(", ");
builder.AppendParameter("bob@example.com");
builder.Append(")");

builder.Compile();
await batch.ExecuteNonQueryAsync();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/CommandBuilderSamples.cs#L58-L82' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlserver_batch_builder' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The SQL Server builder uses named parameters: `INSERT INTO people (name, email) VALUES (@p0, @p1)`.

## CommandBuilderBase (Core)

The `Weasel.Core` library provides `CommandBuilderBase<TCommand, TParameter, TParameterType>` as a generic base for building single `DbCommand` objects. This is used internally by the migration infrastructure when querying database metadata:

<!-- snippet: sample_command_builder_internal_usage -->
<a id='snippet-sample_command_builder_internal_usage'></a>
```cs
// Used internally -- you typically interact with BatchBuilder instead
var cmd = connection.CreateCommand();
var cmdBuilder = new Weasel.Core.DbCommandBuilder(cmd);
schemaObject.ConfigureQueryCommand(cmdBuilder);
var reader = await Weasel.Core.DbCommandBuilderExtensions.ExecuteReaderAsync(connection, cmdBuilder, ct: ct);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/CommandBuilderSamples.cs#L107-L113' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_command_builder_internal_usage' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Grouped Parameters

For scenarios where you need to build parameter sets (e.g., bulk inserts with value lists), use `CreateGroupedParameterBuilder()`:

<!-- snippet: sample_grouped_parameters -->
<a id='snippet-sample_grouped_parameters'></a>
```cs
var group = builder.CreateGroupedParameterBuilder(',');
group.AppendParameter("value1");
group.AppendParameter("value2");
group.AppendParameter("value3");
// Produces: $1,$2,$3 (PostgreSQL) or @p0,@p1,@p2 (SQL Server)
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/CommandBuilderSamples.cs#L90-L96' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_grouped_parameters' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Parameter Types

A command builder types each parameter from the provider's CLR-to-parameter-type map before the
driver ever sees it. Which call you use decides where that type comes from:

| Call | Where the type comes from |
|---|---|
| `AppendParameter(value, dbType)` | the type you pass |
| `AppendParameter(int / Guid / string)` | the provider's map, for that one type |
| `AppendParameter((object)value)` | nowhere — the parameter is left untyped and the driver infers it |
| `AddNamedParameter(name, (object)value)` | the provider's map, from the value |
| `AddParameters(object)` / `AddParameters(dictionary)` | the provider's map, from each value |

When a provider has no mapping for a type, it **defers**: the parameter is left untyped and the
driver infers it from the value. That is deliberate — a provider guessing a catch-all type is worse
than saying nothing, because the server then has to convert. On SQL Server the catch-all is
`sql_variant`, which outranks every ordinary type in data type precedence, so comparing one against
a column converts **the column**, once per row, and an indexed lookup becomes a scan.

`TryGetDbType` and `TryGetDbTypeForValue` return `null` for an unmapped type, so a caller can tell
"no mapping" from a real answer. `ToParameterType` and `ToParameterTypeForValue` throw instead; use
them only where deferring is not acceptable.

Two cases resolve from the value rather than its type, because a per-type map cannot express them:
PostgreSQL picks `timestamptz` over `timestamp` from a `DateTime.Kind`, and picks an array's element
type from its first element. Those only work through the value-aware calls in the table above.

To add a mapping of your own, or to override one, call `RegisterMapping(type, databaseType,
parameterType)` on the provider before you build any commands. Passing `null` as the parameter type
registers the type as deliberately deferred.

### Strings on SQL Server are bound as non-Unicode, on purpose

The two builders disagree about what a .NET `string` is, and the disagreement is deliberate:

| Provider | Parameter type | Sent as |
|---|---|---|
| `Weasel.SqlServer` | `SqlDbType.VarChar` | `varchar` |
| the generic `DbCommandBuilder` | `DbType.String` | `nvarchar` (SqlClient's mapping) |

`nvarchar` outranks `varchar` in SQL Server's data type precedence, so the direction decides which
side of a comparison gets converted — and only one direction converts the column. Measured on SQL
Server 2025 by reading the cached plan out of `sys.dm_exec_query_plan`, against an indexed column of
5,000 rows:

| Parameter | Column | Plan |
|---|---|---|
| `VarChar` | `varchar(100)` | Index Seek |
| `VarChar` | `nvarchar(100)` | Index Seek, `CONVERT_IMPLICIT` on the **parameter** |
| `NVarChar` | `nvarchar(100)` | Index Seek |
| `NVarChar` | `varchar(100)` | **Index Scan**, `CONVERT_IMPLICIT(nvarchar(100), [probe])` on the **column** |

So `varchar` is the safe direction: it seeks against either column shape, because converting one
parameter is free and converting every row is not. `nvarchar` is dangerous specifically against the
`varchar` column that **Weasel's own DDL generates** for a `string`.

Both defaults are left as they are. Changing the generic provider to `DbType.AnsiString` would make
it seek-safe too, but it would silently truncate non-Latin1 text — a data bug traded for a
performance one, which is the wrong direction. Changing the generated column type to `nvarchar`
would be correct and seek-clean, but it is a schema diff and an `ALTER` for every existing `varchar`
column downstream.

If you bind strings through the generic `DbCommandBuilder` against SQL Server and your columns are
`varchar`, pass the parameter type explicitly:

```csharp
builder.AppendParameter(value, DbType.AnsiString);
```

Downstream, for reference: Polecat declares `varchar(250)` and `varchar(500)` for its identity and
metadata columns and `nvarchar(max)` for JSON and flat-table string columns. `Weasel.SqlServer`'s
`VarChar` default seeks against both of those shapes, so nothing downstream is paying for the
column-converting direction today.

A `char` is the one deliberate exception: `Weasel.SqlServer` binds it as `SqlDbType.NChar` against
an `nchar(1)` column. It had no mapping at all before weasel#760, so there was no behaviour to
preserve, the parameter and the column agree with each other, and `nchar` cannot lose a character.
The asymmetry with `string` is intentional rather than an oversight. See weasel#762.

## When to Use What

| Scenario | Tool |
|----------|------|
| Single query or command | `CommandBuilderBase` / provider `DbCommandBuilder` |
| Multiple commands in one round trip | `BatchBuilder` |
| Schema introspection (internal) | `DbCommandBuilder` via `ConfigureQueryCommand()` |
