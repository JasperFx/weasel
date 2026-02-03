# CLAUDE.md - Weasel Project Guide

## What is Weasel?

Weasel is a low-level database abstraction and schema migration library for .NET, extracted from the [Marten](https://martendb.io) project. It provides a unified API for managing database schemas across PostgreSQL, SQL Server, Oracle, and SQLite.

**Core Capabilities:**
- Programmatic definition of database objects (tables, sequences, functions, stored procedures)
- Schema migrations with delta detection (what changed between expected and actual state)
- DDL generation and validation
- Multi-tenancy support
- CLI tools for migrations

## Technology Stack

- **Language:** C# 13.0 / .NET 8.0, 9.0, 10.0
- **Nullable Reference Types:** Enabled (required throughout)
- **PostgreSQL:** Npgsql driver with NetTopologySuite support
- **SQL Server:** Microsoft.Data.SqlClient
- **Oracle:** Oracle.ManagedDataAccess.Core
- **SQLite:** Microsoft.Data.Sqlite with JSON1 extension support
- **Testing:** xUnit, Shouldly, NSubstitute
- **Build:** Nuke.Build

## Project Structure

```
src/
├── Weasel.Core/              # Core abstractions (ISchemaObject, Migrator, SchemaMigration)
│   ├── Migrations/           # Migration infrastructure (IDatabase interface)
│   ├── CommandLine/          # CLI tools
│   └── MultiTenancy/         # Multi-tenant support
├── Weasel.Postgresql/        # PostgreSQL implementation
│   ├── Tables/               # Table handling with partitioning
│   │   └── Partitioning/     # Hash, Range, List partition strategies
│   └── Functions/            # PL/pgSQL function support
├── Weasel.SqlServer/         # SQL Server implementation
│   ├── Tables/               # Table handling
│   ├── Procedures/           # Stored procedure support
│   └── Functions/            # T-SQL function support
├── Weasel.Oracle/            # Oracle implementation
│   └── Tables/               # Table handling with Oracle-specific features
├── Weasel.Sqlite/            # SQLite implementation (NEW!)
│   └── Tables/               # Table handling with JSON support
└── *Tests/                   # Test projects for each library
```

## Build Commands

```bash
# Restore and build
dotnet restore
dotnet build

# Build release
dotnet build -c Release

# Using Nuke build scripts
./build.sh          # Unix/macOS
.\build.ps1         # Windows PowerShell
```

## Test Commands

```bash
# Run all tests
dotnet test

# Run specific test projects
dotnet test src/Weasel.Core.Tests/Weasel.Core.Tests.csproj
dotnet test src/Weasel.Postgresql.Tests/Weasel.Postgresql.Tests.csproj
dotnet test src/Weasel.SqlServer.Tests/Weasel.SqlServer.Tests.csproj
dotnet test src/Weasel.Sqlite.Tests/Weasel.Sqlite.Tests.csproj

# Target specific framework
dotnet test --framework net8.0
```

**Database Setup for Integration Tests:**
```bash
docker compose up  # For PostgreSQL, SQL Server, Oracle
# SQLite tests require no setup - they use in-memory or temporary databases
```

**Connection Strings (environment variables):**
- `weasel_postgresql_testing_database` - PostgreSQL connection
- `weasel_sqlserver_testing_database` - SQL Server connection
- SQLite tests don't require environment variables

## Key Abstractions

### ISchemaObject Interface
All database objects (tables, sequences, functions) implement this:
- `WriteCreateStatement()` - DDL to create the object
- `WriteDropStatement()` - DDL to drop the object
- `CreateDeltaAsync()` - Detect differences between expected and actual state

### Provider Pattern
Each database has:
- `*Database` class implementing `IDatabase`
- `*Migrator` class with SQL formatting rules
- `*Provider` class for schema introspection

### SchemaMigration
Aggregates deltas across objects, determines action needed: `None`, `Update`, `Create`, `Delete`, `Recreate`

### CreationStyle Enum
- `CreateIfNotExists` - Safe creation (default)
- `DropThenCreate` - Drop existing first

## Code Style

Configured via `.editorconfig`:
- 4 spaces indentation
- LF line endings
- Predefined types for primitives (`int` not `Int32`)
- No unnecessary `this.` qualification
- Accessibility modifiers always required
- Object/collection initializers preferred
- Null propagation and coalesce expressions preferred
- Use Pascal casing for internal or public members
- Use Camel casing for private or protected members
- Use an underscore prefix for private fields

## Common Patterns

**Async with Cancellation:** All I/O operations use `async/await` with `CancellationToken`

**Fluent Configuration:** Builders and extension methods for object setup

**Delta Detection:** Compare expected schema object state against actual database state

## Key Files

| File | Purpose |
|------|---------|
| `Weasel.Core/ISchemaObject.cs` | Core interface for all database objects |
| `Weasel.Core/Migrations/IDatabase.cs` | Database abstraction with migration support |
| `Weasel.Core/SchemaMigration.cs` | Schema change detection and aggregation |
| `Weasel.Postgresql/PostgresqlDatabase.cs` | PostgreSQL implementation |
| `Weasel.SqlServer/SqlServerMigrator.cs` | SQL Server formatting rules |

## CI/CD

GitHub Actions workflows:
- `ci-build-postgres.yml` - PostgreSQL CI (Ubuntu, multiple PostgreSQL versions)
- `ci-build-mssql.yml` - SQL Server CI (Ubuntu)
- `ci-build-oracle.yml` - Oracle CI (Ubuntu)
- `ci-build-sqlite.yml` - SQLite CI (Ubuntu, Windows, macOS - multi-platform)
- `publish_nuget.yml` - NuGet publishing

**SQLite CI Workflow:**
The SQLite workflow tests on multiple operating systems (Ubuntu, Windows, macOS) and all supported .NET versions (8.0, 9.0, 10.0) to ensure cross-platform compatibility. SQLite tests require no database services since SQLite is file-based and embedded.

## Version

Defined in `Directory.Build.props`

## SQLite Support

### Overview

Weasel.Sqlite provides full support for SQLite databases with JSON1 extension compatibility. SQLite is a lightweight, serverless database engine perfect for embedded applications, mobile apps, and local development.

### Key Features

- **JSON Support**: Full TEXT-based JSON storage with JSON1 extension functions
- **AUTOINCREMENT**: INTEGER PRIMARY KEY AUTOINCREMENT support
- **Generated Columns**: GENERATED ALWAYS AS ... STORED/VIRTUAL (SQLite 3.31+)
- **Foreign Keys**: Inline constraint definitions (no ALTER TABLE support)
- **Expression Indexes**: Support for json_extract() and custom expressions
- **Partial Indexes**: WHERE clause support for filtered indexes
- **STRICT Tables**: Strict type checking (SQLite 3.37+)
- **WITHOUT ROWID**: Performance optimization for tables

### SQLite-Specific Limitations

SQLite has limited ALTER TABLE support compared to other databases:

- **No ALTER TABLE ADD CONSTRAINT**: Foreign keys must be defined at table creation
- **No ALTER TABLE DROP CONSTRAINT**: Requires table recreation
- **Limited ALTER COLUMN**: Most column changes require table recreation
- **ALTER TABLE ADD COLUMN**: Supported (column must be nullable or have DEFAULT)
- **ALTER TABLE DROP COLUMN**: Supported (SQLite 3.35+)
- **ALTER TABLE RENAME COLUMN**: Supported (SQLite 3.25+)

### Usage Example

```csharp
using Weasel.Sqlite;
using Weasel.Sqlite.Tables;
using Microsoft.Data.Sqlite;

// Create a table with JSON support
var table = new Table("users");

// Add columns
table.AddColumn("id", "INTEGER").AsPrimaryKey().AutoIncrement();
table.AddColumn("name", "TEXT").NotNull();
table.AddColumn("email", "TEXT").NotNull();
table.AddColumn("settings", "TEXT"); // JSON column

// Add a generated column
table.AddColumn("email_domain", "TEXT")
    .GeneratedAs("substr(email, instr(email, '@') + 1)", GeneratedColumnType.Stored);

// Create an index on JSON path
var emailIndex = new IndexDefinition("idx_email") { IsUnique = true };
emailIndex.AgainstColumns("email");
table.Indexes.Add(emailIndex);

// Create an expression index for JSON
var settingsIndex = new IndexDefinition("idx_settings_theme");
settingsIndex.ForJsonPath("settings", "$.theme");
table.Indexes.Add(settingsIndex);

// Enable STRICT mode for type safety
table.StrictTypes = true;

// Generate DDL
using var writer = new StringWriter();
var migrator = new SqliteMigrator();
table.WriteCreateStatement(migrator, writer);
Console.WriteLine(writer.ToString());
```

### Testing

```bash
# Run SQLite tests (no Docker required - in-memory database)
dotnet test src/Weasel.Sqlite.Tests/Weasel.Sqlite.Tests.csproj

# Target specific framework
dotnet test --framework net9.0
```

### Type Mappings

| .NET Type | SQLite Type | Storage Class |
|-----------|-------------|---------------|
| `int`, `long`, `short`, `byte` | INTEGER | INTEGER |
| `bool` | INTEGER | INTEGER |
| `float`, `double`, `decimal` | REAL | REAL |
| `string`, `char` | TEXT | TEXT |
| `DateTime`, `DateTimeOffset` | TEXT | TEXT (ISO8601) |
| `Guid` | TEXT | TEXT |
| `byte[]` | BLOB | BLOB |
| **Complex types (JSON)** | **TEXT** | **TEXT** |

### JSON1 Extension

Weasel.Sqlite assumes JSON1 extension is available. To verify:

```sql
SELECT json('{"test": true}');
```

Most SQLite builds include JSON1 by default. If not available, you'll need to recompile SQLite with JSON1 enabled.

### Implemented Components

✅ **Complete:**
- SqliteProvider - Type mappings with TEXT fallback for JSON
- SqliteMigrator - DDL generation with transaction support
- SqliteObjectName - Case-insensitive name parsing
- SchemaUtils - Identifier quoting with reserved keywords
- CommandBuilder & CommandExtensions - SQL helpers
- TableColumn - AUTOINCREMENT, generated columns, constraints
- ForeignKey - Inline FK definitions (no ALTER TABLE)
- IndexDefinition - Expression indexes, JSON path support, partial indexes
- Table - Basic DDL generation and fluent API
- **View** - Full view support with CREATE/DROP/ALTER (via drop+recreate)
- **ViewDelta** - View change detection and migration generation
- **SqlitePragmaSettings** - Comprehensive PRAGMA configuration with Default, HighPerformance, and HighSafety presets
- **SqliteExtensionSettings** - Extension loading support for SpatiaLite, FTS5, ICU, and custom extensions
- **SqliteDatabase** - PRAGMA settings and extension integration with automatic application to connections

🚧 **To Be Implemented:**
- Table.Deltas.cs - Delta detection logic
- Table.FetchExisting.cs - Schema introspection from sqlite_master
- TableDelta.cs - Difference calculation and migration generation
- Full integration tests for tables

### Migration Strategy

Due to SQLite's ALTER TABLE limitations, schema changes often require table recreation:

1. Create new table with desired schema
2. Copy data: `INSERT INTO new_table SELECT * FROM old_table`
3. Drop old table
4. Rename new table

This is handled automatically by the delta detection system (when fully implemented).

### Connection String

```csharp
// In-memory database
var connectionString = "Data Source=:memory:";

// File-based database
var connectionString = "Data Source=myapp.db";

// With foreign keys enabled
var connectionString = "Data Source=myapp.db;Foreign Keys=True";
```

### PRAGMA Settings

Weasel.Sqlite provides comprehensive PRAGMA configuration for optimal database performance and reliability through the `SqlitePragmaSettings` class. PRAGMA settings control critical database behavior like journaling, caching, and synchronization.

**Three Built-in Presets:**

1. **Default** - Balanced settings for general-purpose applications
   - WAL (Write-Ahead Logging) journal mode for concurrent reads
   - NORMAL synchronous mode for good safety/performance balance
   - 64MB cache size
   - Foreign key constraints enabled
   - Incremental auto-vacuum

2. **HighPerformance** - Maximum speed (reduced safety)
   - WAL journal mode
   - OFF synchronous mode (risk of corruption on power loss)
   - 128MB cache size
   - No auto-vacuum overhead

3. **HighSafety** - Maximum durability and data integrity
   - WAL journal mode
   - FULL synchronous mode
   - Secure delete enabled
   - Full auto-vacuum
   - Longer busy timeout (10 seconds)

**Usage with SqliteDatabase:**

```csharp
// Use default settings
var database = new MyDatabase(
    logger,
    AutoCreate.CreateOrUpdate,
    new SqliteMigrator(),
    "my-db",
    "Data Source=myapp.db"
);

// Use high-performance settings
var database = new MyDatabase(
    logger,
    AutoCreate.CreateOrUpdate,
    new SqliteMigrator(),
    "my-db",
    "Data Source=myapp.db",
    SqlitePragmaSettings.HighPerformance
);

// Custom settings
var customSettings = new SqlitePragmaSettings
{
    JournalMode = JournalMode.WAL,
    Synchronous = SynchronousMode.NORMAL,
    CacheSize = -32000, // 32MB (negative = KiB)
    ForeignKeys = true,
    BusyTimeout = 5000,
    WalAutoCheckpoint = 1000 // Checkpoint every 1000 pages
};

var database = new MyDatabase(
    logger,
    AutoCreate.CreateOrUpdate,
    new SqliteMigrator(),
    "my-db",
    "Data Source=myapp.db",
    customSettings
);

// Apply PRAGMA settings to a connection
await using var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();
await database.ApplyPragmaSettingsAsync(connection);
```

**Important PRAGMA Settings:**

| Setting | Default | Description |
|---------|---------|-------------|
| `JournalMode` | WAL | Transaction logging mode. WAL allows concurrent readers with single writer |
| `Synchronous` | NORMAL | How often SQLite syncs to disk. FULL=safest, NORMAL=balanced, OFF=fastest |
| `CacheSize` | -64000 | Cache size in KiB (negative) or pages (positive). -64000 = 64MB |
| `TempStore` | MEMORY | Where temporary tables are stored (MEMORY is fastest) |
| `MmapSize` | 268435456 | Memory-mapped I/O size in bytes (256MB default). Improves read performance |
| `PageSize` | 4096 | Database page size. Must be set before database creation. 4096 optimal for modern systems |
| `ForeignKeys` | true | Enable foreign key constraint enforcement (disabled by default in SQLite) |
| `AutoVacuum` | INCREMENTAL | Automatic space reclamation. INCREMENTAL allows manual shrinking |
| `BusyTimeout` | 5000 | Milliseconds to wait when database is locked |
| `SecureDelete` | false | Overwrite deleted data with zeros (slower but more secure) |
| `WalAutoCheckpoint` | null | Pages to accumulate before auto-checkpoint (WAL mode only) |

**Generate SQL Script for Diagnostics:**

```csharp
var settings = SqlitePragmaSettings.Default;
var script = settings.ToSqlScript();
Console.WriteLine(script);

// Output:
// -- SQLite PRAGMA Settings
// PRAGMA journal_mode = WAL;
// PRAGMA synchronous = NORMAL;
// PRAGMA cache_size = -64000;
// PRAGMA temp_store = 2;
// PRAGMA mmap_size = 268435456;
// PRAGMA page_size = 4096;
// PRAGMA foreign_keys = ON;
// PRAGMA auto_vacuum = 2;
// PRAGMA busy_timeout = 5000;
// PRAGMA secure_delete = OFF;
// PRAGMA case_sensitive_like = OFF;
```

**Notes:**

- PRAGMA settings are applied **after** opening a connection (not in connection string)
- Some settings like `page_size` and `auto_vacuum` can only be set before database creation
- WAL mode doesn't work with in-memory databases (`:memory:`)
- PRAGMA settings don't persist in SQLite - they must be applied each time a connection opens
- Foreign key constraints are disabled by default in SQLite and must be explicitly enabled

### SQLite Extensions

Weasel.Sqlite provides support for loading SQLite extensions to add additional functionality like spatial data support (SpatiaLite), full-text search, encryption, or custom functions.

**Extension Configuration:**

```csharp
// Configure extensions to load
var extensionSettings = new SqliteExtensionSettings();

// Add SpatiaLite extension (spatial data support)
extensionSettings.AddExtension("mod_spatialite");

// Add extension with custom entry point
extensionSettings.AddExtension("my_custom_extension", "sqlite3_my_extension_init");

// Add multiple extensions
extensionSettings.AddExtension("extension1");
extensionSettings.AddExtension("extension2");
```

**Using Extensions with SqliteDatabase:**

```csharp
// Create database with extension support
var extensionSettings = new SqliteExtensionSettings();
extensionSettings.AddExtension("mod_spatialite");

var database = new MyDatabase(
    logger,
    AutoCreate.CreateOrUpdate,
    new SqliteMigrator(),
    "my-db",
    "Data Source=myapp.db",
    SqlitePragmaSettings.Default,
    extensionSettings
);

// Apply extensions to a connection
await using var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();
await database.ApplyExtensionsAsync(connection);

// Or apply both PRAGMA settings and extensions
await database.ApplyConnectionSettingsAsync(connection);

// Now you can use extension functions
var cmd = connection.CreateCommand();
cmd.CommandText = "SELECT spatialite_version()";
var version = (string)cmd.ExecuteScalar();
```

**Directly Using Extension Settings:**

```csharp
// Apply extension settings to any connection
var settings = new SqliteExtensionSettings();
settings.AddExtension("mod_spatialite");

await using var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();
await settings.ApplyToConnectionAsync(connection);
```

**Common SQLite Extensions:**

| Extension | Library Name | Description |
|-----------|--------------|-------------|
| **SpatiaLite** | `mod_spatialite` | Spatial data support (geometry types, spatial indexes, GIS functions) |
| **FTS5** | Built-in | Full-text search (compile-time option, usually included) |
| **JSON1** | Built-in | JSON functions (usually included by default) |
| **R*Tree** | Built-in | Spatial indexing (compile-time option) |
| **ICU** | `icu` | International Components for Unicode (collation, case mapping) |
| **SQLCipher** | Custom build | Database encryption |

**Platform Considerations:**

SQLite loads extensions using platform-specific native library loading mechanisms. You may need to configure environment variables:

- **Windows**: Add extension directory to `PATH`
- **Linux**: Add extension directory to `LD_LIBRARY_PATH`
- **macOS**: Add extension directory to `DYLD_LIBRARY_PATH`

```bash
# Linux example
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

# macOS example
export DYLD_LIBRARY_PATH=/usr/local/lib:$DYLD_LIBRARY_PATH

# Windows PowerShell example
$env:PATH += ";C:\sqlite\extensions"
```

**Full Path vs Library Name:**

```csharp
// Use library name (must be in system path)
settings.AddExtension("mod_spatialite");

// Or use full path
settings.AddExtension("/usr/local/lib/mod_spatialite.so");
settings.AddExtension("/usr/local/lib/mod_spatialite.dylib"); // macOS
settings.AddExtension("C:\\sqlite\\extensions\\mod_spatialite.dll"); // Windows
```

**Security:**

For security, `EnableExtensions` is automatically disabled after all extensions are loaded. This prevents dynamic extension loading during query execution.

**Error Handling:**

If an extension fails to load, an `InvalidOperationException` is thrown with details about the library path and suggestions for fixing path issues:

```csharp
try
{
    await settings.ApplyToConnectionAsync(connection);
}
catch (InvalidOperationException ex)
{
    // Message will include:
    // - Extension library path
    // - Entry point (if specified)
    // - Guidance about PATH/LD_LIBRARY_PATH/DYLD_LIBRARY_PATH
    Console.WriteLine(ex.Message);
}
```

### Views

Weasel.Sqlite provides comprehensive support for SQLite views with schema migration capabilities:

```csharp
using Weasel.Sqlite.Views;

// Create a simple view
var view = new View("active_users", "SELECT id, name, email FROM users WHERE active = 1");

// Generate DDL
var migrator = new SqliteMigrator();
var writer = new StringWriter();
view.WriteCreateStatement(migrator, writer);
Console.WriteLine(writer.ToString());
// Output:
// DROP VIEW IF EXISTS main.active_users;
// CREATE VIEW main.active_users AS SELECT id, name, email FROM users WHERE active = 1;

// Create a view with JOIN
var orderSummaryView = new View("user_order_summary", @"
    SELECT u.id, u.name, COUNT(o.id) as order_count, SUM(o.amount) as total_amount
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    GROUP BY u.id, u.name");

// Create a view with JSON extraction
var productDetailsView = new View("product_details", @"
    SELECT
        id,
        name,
        json_extract(metadata, '$.category') as category,
        json_extract(metadata, '$.price') as price
    FROM products");

// Check if a view exists in the database
await using var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();

var exists = await view.ExistsInDatabaseAsync(connection);

// Fetch existing view definition from database
var existing = await view.FetchExistingAsync(connection);

// Detect changes (delta)
var delta = new ViewDelta(expectedView, actualView);
if (delta.Difference == SchemaPatchDifference.None)
{
    // View matches
}
else if (delta.Difference == SchemaPatchDifference.Create)
{
    // View doesn't exist - create it
}
else if (delta.Difference == SchemaPatchDifference.Update)
{
    // View exists but has changed - drop and recreate
}
```

**View Features:**
- Full CREATE VIEW and DROP VIEW support
- Schema introspection via `sqlite_master` table
- Delta detection with whitespace-insensitive SQL comparison
- Support for complex views (JOINs, aggregations, CTEs)
- JSON extraction via `json_extract()` function
- Automatic view recreation on changes (SQLite doesn't support ALTER VIEW)
- Case-insensitive identifier handling

**View Limitations:**
- SQLite views are read-only (no INSERT/UPDATE/DELETE)
- No materialized views (use tables with triggers instead)
- Changes require DROP + CREATE (handled automatically by ViewDelta)
- Views cannot reference temporary tables

### Custom Functions

Unlike PostgreSQL and SQL Server, SQLite does not store user-defined functions as database objects. Instead, functions are registered programmatically via the `Microsoft.Data.Sqlite` API and exist only for the lifetime of the connection.

```csharp
using Microsoft.Data.Sqlite;

await using var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();

// Register a scalar function
connection.CreateFunction(
    name: "double_value",
    (int value) => value * 2,
    isDeterministic: true);

// Use the function in queries
var cmd = connection.CreateCommand();
cmd.CommandText = "SELECT id, double_value(amount) FROM orders";
await using var reader = await cmd.ExecuteReaderAsync();

// Register an aggregate function
connection.CreateAggregate(
    name: "string_concat",
    seed: () => new List<string>(),
    func: (List<string> list, string value) =>
    {
        list.Add(value);
        return list;
    },
    resultSelector: list => string.Join(", ", list),
    isDeterministic: true);
```

**Function Characteristics:**
- Functions are connection-scoped (not stored in the database)
- Must be re-registered for each new connection
- Support scalar and aggregate functions
- Deterministic functions can be optimized by SQLite
- No ALTER FUNCTION or DROP FUNCTION SQL (functions unregister when connection closes)

**Best Practices:**
- Register commonly used functions in a connection factory or startup code
- Use extension methods to encapsulate function registration logic
- Consider wrapping function registration in a helper class for reuse
- Document custom functions in application code since they're not visible in the database schema

### Important Notes

- **Foreign Keys**: Disabled by default in SQLite. Enable with `PRAGMA foreign_keys = ON` or connection string parameter
- **Case Sensitivity**: SQLite is case-insensitive by default for identifiers
- **Type Affinity**: SQLite uses type affinity rather than strict typing (unless STRICT mode is enabled)
- **JSON Storage**: Uses TEXT columns with JSON1 functions for querying
- **Views**: Read-only, no ALTER VIEW support (changes require drop+recreate)
- **Custom Functions**: Registered programmatically per connection (not stored in database)
