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
<a id='snippet-sample_ICommandBuilder_interface'></a>
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
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/CommandBuilderSamples.cs#L6-L19' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ICommandBuilder_interface' title='Start of snippet'>anchor</a></sup>
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

## When to Use What

| Scenario | Tool |
|----------|------|
| Single query or command | `CommandBuilderBase` / provider `DbCommandBuilder` |
| Multiple commands in one round trip | `BatchBuilder` |
| Schema introspection (internal) | `DbCommandBuilder` via `ConfigureQueryCommand()` |
