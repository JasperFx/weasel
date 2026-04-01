# PRAGMA Settings

The `SqlitePragmaSettings` class provides comprehensive PRAGMA configuration for controlling SQLite database behavior including journaling, caching, and synchronization.

## Built-in Presets

### Default

Balanced settings for general-purpose applications:

- WAL journal mode for concurrent reads
- NORMAL synchronous mode
- 64MB cache, foreign keys enabled, incremental auto-vacuum

### HighPerformance

Maximum speed with reduced safety guarantees:

<!-- snippet: sample_sqlite_pragma_high_performance -->
<a id='snippet-sample_sqlite_pragma_high_performance'></a>
```cs
var settings = SqlitePragmaSettings.HighPerformance;
var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();
await settings.ApplyToConnectionAsync(connection);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L293-L298' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_pragma_high_performance' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

- WAL journal mode, OFF synchronous mode
- 128MB cache, no auto-vacuum overhead

::: danger
HighPerformance uses `synchronous = OFF`, which risks database corruption on power loss or OS crash. Use only when data can be regenerated.
:::

### HighSafety

Maximum durability and data integrity:

<!-- snippet: sample_sqlite_pragma_high_safety -->
<a id='snippet-sample_sqlite_pragma_high_safety'></a>
```cs
var settings = SqlitePragmaSettings.HighSafety;
var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();
await settings.ApplyToConnectionAsync(connection);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L303-L308' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_pragma_high_safety' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

- WAL journal mode, FULL synchronous mode
- Secure delete enabled, full auto-vacuum, 10-second busy timeout

## Settings Reference

| Setting | Default | Description |
|---------|---------|-------------|
| `JournalMode` | WAL | Transaction logging. WAL allows concurrent readers with a single writer |
| `Synchronous` | NORMAL | Disk sync frequency. FULL = safest, NORMAL = balanced, OFF = fastest |
| `CacheSize` | -64000 | Cache in KiB (negative) or pages (positive). -64000 = 64MB |
| `TempStore` | MEMORY | Where temporary tables are stored |
| `MmapSize` | 268435456 | Memory-mapped I/O size in bytes (256MB) |
| `PageSize` | 4096 | Database page size. Must be set before database creation |
| `ForeignKeys` | true | Enable foreign key constraint enforcement |
| `AutoVacuum` | INCREMENTAL | Automatic space reclamation strategy |
| `BusyTimeout` | 5000 | Milliseconds to wait when database is locked |
| `SecureDelete` | false | Overwrite deleted data with zeros |
| `WalAutoCheckpoint` | null | Pages before auto-checkpoint (WAL mode only) |

## Custom Configuration

<!-- snippet: sample_sqlite_pragma_custom_configuration -->
<a id='snippet-sample_sqlite_pragma_custom_configuration'></a>
```cs
var settings = new SqlitePragmaSettings
{
    JournalMode = JournalMode.WAL,
    Synchronous = SynchronousMode.NORMAL,
    CacheSize = -32000, // 32MB
    ForeignKeys = true,
    BusyTimeout = 5000,
    WalAutoCheckpoint = 1000
};
var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();
await settings.ApplyToConnectionAsync(connection);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L313-L326' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_pragma_custom_configuration' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Applying to an Existing Connection

<!-- snippet: sample_sqlite_pragma_apply_existing_connection -->
<a id='snippet-sample_sqlite_pragma_apply_existing_connection'></a>
```cs
var settings = SqlitePragmaSettings.Default;
var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();
await settings.ApplyToConnectionAsync(connection);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L331-L336' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_pragma_apply_existing_connection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generating a SQL Script

<!-- snippet: sample_sqlite_pragma_sql_script -->
<a id='snippet-sample_sqlite_pragma_sql_script'></a>
```cs
var settings = SqlitePragmaSettings.Default;
Console.WriteLine(settings.ToSqlScript());

// -- SQLite PRAGMA Settings
// PRAGMA journal_mode = WAL;
// PRAGMA synchronous = NORMAL;
// PRAGMA cache_size = -64000;
// ...
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L341-L350' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_pragma_sql_script' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Important Notes

- PRAGMA settings are per-connection and must be applied every time a connection opens
- Some settings (`page_size`, `auto_vacuum`) can only be set before the database is created
- WAL mode does not work with in-memory databases (`:memory:`)
- Foreign key constraints are disabled by default in SQLite and must be explicitly enabled
- Use [SqliteHelper](/sqlite/helper) for consistent PRAGMA application across your application
