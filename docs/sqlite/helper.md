# SqliteHelper

The `SqliteHelper` static class simplifies connection creation with automatic PRAGMA configuration and provides a factory for the `SqliteMigrator`.

## Creating Connections

### Basic Connection with Default PRAGMAs

<!-- snippet: sample_sqlite_helper_basic_connection -->
<a id='snippet-sample_sqlite_helper_basic_connection'></a>
```cs
var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();

// Apply default PRAGMA settings (WAL mode, NORMAL sync, 64MB cache, foreign keys enabled)
await SqlitePragmaSettings.Default.ApplyToConnectionAsync(connection);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L357-L363' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_helper_basic_connection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This opens the connection and applies `SqlitePragmaSettings.Default` (WAL mode, NORMAL sync, 64MB cache, foreign keys enabled).

### Custom PRAGMA Configuration

Pass an `Action<SqlitePragmaSettings>` to configure specific settings:

<!-- snippet: sample_sqlite_helper_custom_pragmas -->
<a id='snippet-sample_sqlite_helper_custom_pragmas'></a>
```cs
var settings = new SqlitePragmaSettings
{
    JournalMode = JournalMode.WAL,
    ForeignKeys = true,
    CacheSize = -64000 // 64MB
};
var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();
await settings.ApplyToConnectionAsync(connection);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L368-L378' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_helper_custom_pragmas' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

### Using Presets

<!-- snippet: sample_sqlite_helper_presets -->
<a id='snippet-sample_sqlite_helper_presets'></a>
```cs
// High performance (reduced safety)
var highPerfConn = new SqliteConnection("Data Source=myapp.db");
await highPerfConn.OpenAsync();
await SqlitePragmaSettings.HighPerformance.ApplyToConnectionAsync(highPerfConn);

// High safety (maximum durability)
var highSafetyConn = new SqliteConnection("Data Source=myapp.db");
await highSafetyConn.OpenAsync();
await SqlitePragmaSettings.HighSafety.ApplyToConnectionAsync(highSafetyConn);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L383-L393' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_helper_presets' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

See [PRAGMA Settings](/sqlite/pragmas) for details on each preset.

## Creating a Migrator

Use `CreateMigrator()` to get a `SqliteMigrator` instance for DDL generation:

<!-- snippet: sample_sqlite_helper_create_migrator -->
<a id='snippet-sample_sqlite_helper_create_migrator'></a>
```cs
var migrator = new SqliteMigrator();

var table = new Table("users");
table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
table.AddColumn<string>("name").NotNull();

var writer = new StringWriter();
table.WriteCreateStatement(migrator, writer);
Console.WriteLine(writer.ToString());
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L398-L408' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_helper_create_migrator' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Connection String Examples

<!-- snippet: sample_sqlite_connection_string_examples -->
<a id='snippet-sample_sqlite_connection_string_examples'></a>
```cs
// In-memory database (lost when connection closes)
var inMemory = "Data Source=:memory:";

// File-based database
var fileBased = "Data Source=myapp.db";

// Shared cache for multiple connections to the same in-memory database
var sharedCache = "Data Source=myapp;Mode=Memory;Cache=Shared";

// Read-only access
var readOnly = "Data Source=myapp.db;Mode=ReadOnly";
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L413-L425' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_connection_string_examples' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Method Reference

| Method | Description |
|--------|-------------|
| `CreateConnectionAsync(string, Action<SqlitePragmaSettings>?, CancellationToken?)` | Opens a `SqliteConnection` with PRAGMA settings applied |
| `CreateMigrator()` | Returns a new `SqliteMigrator` for DDL generation |

## Recommended Usage

Configure PRAGMAs through `SqliteHelper` rather than in connection strings or with raw SQL. This provides type safety, validation, and consistent settings across your application:

<!-- snippet: sample_sqlite_helper_recommended_usage -->
<a id='snippet-sample_sqlite_helper_recommended_usage'></a>
```cs
// Preferred: type-safe PRAGMA configuration
var settings = new SqlitePragmaSettings
{
    JournalMode = JournalMode.WAL,
    Synchronous = SynchronousMode.NORMAL,
    ForeignKeys = true
};
var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();
await settings.ApplyToConnectionAsync(connection);

// Avoid: raw PRAGMA statements
// var cmd = connection.CreateCommand();
// cmd.CommandText = "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;";
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L430-L445' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_helper_recommended_usage' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
