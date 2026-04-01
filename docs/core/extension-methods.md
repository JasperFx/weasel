# Extension Methods

Each Weasel provider ships a `SchemaObjectsExtensions` class with convenience methods that make it easy to create, drop, migrate, and apply changes to individual schema objects using a single method call. These extensions wrap the delta detection and DDL generation pipeline behind simple async APIs.

## SchemaObjectsExtensions

Every provider (`Weasel.Postgresql`, `Weasel.SqlServer`, `Weasel.Oracle`, `Weasel.Sqlite`) defines its own parallel set of these extension methods, each typed to the provider's connection class.

### ApplyChangesAsync

Detects differences between the configured schema object and the actual database, then applies any needed DDL:

<!-- snippet: sample_apply_changes_async -->
<a id='snippet-sample_apply_changes_async'></a>
```cs
// PostgreSQL
await pgTable.ApplyChangesAsync(npgsqlConnection);

// SQL Server
await ssTable.ApplyChangesAsync(sqlConnection);

// SQLite
await sqliteTable.ApplyChangesAsync(sqliteConnection);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/ExtensionMethodSamples.cs#L23-L32' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_apply_changes_async' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This is the most common method for keeping a single object in sync with the database. It runs the full detect-and-apply cycle.

### CreateAsync

Generates and executes only the creation DDL, without checking whether the object already exists:

<!-- snippet: sample_create_async -->
<a id='snippet-sample_create_async'></a>
```cs
// PostgreSQL
await pgTable.CreateAsync(npgsqlConnection);

// SQL Server
await ssTable.CreateAsync(sqlConnection);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/ExtensionMethodSamples.cs#L42-L48' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_create_async' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Use this when you know the object does not exist yet (e.g., during initial setup or testing).

### DropAsync

Generates and executes the drop DDL:

<!-- snippet: sample_drop_async -->
<a id='snippet-sample_drop_async'></a>
```cs
// PostgreSQL
await pgTable.DropAsync(npgsqlConnection);

// SQL Server
await ssTable.Drop(sqlConnection);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/ExtensionMethodSamples.cs#L58-L64' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_drop_async' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

### MigrateAsync

Creates or updates the object based on delta detection, respecting an `AutoCreate` policy:

<!-- snippet: sample_migrate_async -->
<a id='snippet-sample_migrate_async'></a>
```cs
// PostgreSQL -- defaults to AutoCreate.CreateOrUpdate
bool changed = await table.MigrateAsync(npgsqlConnection);

// With explicit policy
changed = await table.MigrateAsync(
    npgsqlConnection,
    autoCreate: AutoCreate.CreateOnly
);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/ExtensionMethodSamples.cs#L72-L81' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_migrate_async' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Returns `true` if any changes were applied, `false` if the object was already up to date. You can also migrate an array of objects together:

<!-- snippet: sample_migrate_async_array -->
<a id='snippet-sample_migrate_async_array'></a>
```cs
var objects = new ISchemaObject[] { usersTable, ordersTable, sequence };
bool changed = await objects.MigrateAsync(sqlConnection);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/ExtensionMethodSamples.cs#L91-L94' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_migrate_async_array' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

### EnsureSchemaExists

Creates a database schema if it does not already exist (PostgreSQL and SQL Server):

<!-- snippet: sample_ensure_schema_exists -->
<a id='snippet-sample_ensure_schema_exists'></a>
```cs
// PostgreSQL
await npgsqlConnection.EnsureSchemaExists("myapp");

// SQL Server
await sqlConnection.EnsureSchemaExists("myapp");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/ExtensionMethodSamples.cs#L102-L108' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_ensure_schema_exists' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

SQLite does not need this method because it only supports the built-in `main` and `temp` schemas.

## Full PostgreSQL Example

<!-- snippet: sample_full_postgresql_extension_example -->
<a id='snippet-sample_full_postgresql_extension_example'></a>
```cs
await using var dataSource = NpgsqlDataSource.Create(connectionString);
await using var conn = await dataSource.OpenConnectionAsync();

// Ensure the schema exists
await conn.EnsureSchemaExists("myapp");

// Define a table
var table = new Weasel.Postgresql.Tables.Table(new PostgresqlObjectName("myapp", "people"));
table.AddColumn<int>("id").AsPrimaryKey();
table.AddColumn<string>("name").NotNull();
table.AddColumn<string>("email");

// Apply changes -- creates the table if missing, updates if changed
await table.ApplyChangesAsync(conn);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/ExtensionMethodSamples.cs#L115-L130' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_full_postgresql_extension_example' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Full SQL Server Example

<!-- snippet: sample_full_sqlserver_extension_example -->
<a id='snippet-sample_full_sqlserver_extension_example'></a>
```cs
await using var conn = new SqlConnection(connectionString);
await conn.OpenAsync();

await conn.EnsureSchemaExists("myapp");

var table = new Weasel.SqlServer.Tables.Table(new SqlServerObjectName("myapp", "people"));
table.AddColumn<int>("id").AsPrimaryKey();
table.AddColumn<string>("name").NotNull();
table.AddColumn<string>("email");

await table.ApplyChangesAsync(conn);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/ExtensionMethodSamples.cs#L137-L149' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_full_sqlserver_extension_example' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Full SQLite Example

<!-- snippet: sample_full_sqlite_extension_example -->
<a id='snippet-sample_full_sqlite_extension_example'></a>
```cs
await using var conn = new SqliteConnection("Data Source=myapp.db");
await conn.OpenAsync();

// Apply PRAGMA settings for performance
var pragmas = new SqlitePragmaSettings
{
    JournalMode = JournalMode.WAL,
    ForeignKeys = true
};
await pragmas.ApplyToConnectionAsync(conn);

var table = new Weasel.Sqlite.Tables.Table("people");
table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
table.AddColumn<string>("name").NotNull();
table.AddColumn<string>("email");

await table.ApplyChangesAsync(conn);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/ExtensionMethodSamples.cs#L154-L172' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_full_sqlite_extension_example' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## CommandBuilder Extensions

Beyond schema objects, Weasel also provides extensions on `CommandBuilderBase` for executing commands:

| Method | Purpose |
|--------|---------|
| `ExecuteNonQueryAsync()` | Execute the command without reading results. |
| `ExecuteReaderAsync()` | Execute and return a `DbDataReader`. |
| `FetchListAsync<T>()` | Execute, read all rows, and materialize a `List<T>`. |

These are used internally by Weasel's migration infrastructure and are available for your own database operations.

## Provider Parity

Each provider implements the same set of extension methods with the same signatures (adjusted for connection type). This means switching providers requires only changing the `using` directive and connection type -- the calling code stays the same.

| Extension | PostgreSQL | SQL Server | Oracle | SQLite |
|-----------|-----------|------------|--------|--------|
| `ApplyChangesAsync` | Yes | Yes | Yes | Yes |
| `CreateAsync` | Yes | Yes | Yes | Yes |
| `DropAsync` | Yes | Yes | Yes | Yes |
| `MigrateAsync` | Yes | Yes | Yes | Yes |
| `EnsureSchemaExists` | Yes | Yes | No | No |
