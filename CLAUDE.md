# CLAUDE.md - Weasel Project Guide

## What is Weasel?

Weasel is a low-level database abstraction and schema migration library for .NET, extracted from the [Marten](https://martendb.io) project. It provides a unified API for managing database schemas across PostgreSQL and SQL Server.

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

# Target specific framework
dotnet test --framework net8.0
```

**Database Setup for Integration Tests:**
```bash
docker compose up
```

**Connection Strings (environment variables):**
- `weasel_postgresql_testing_database` - PostgreSQL connection
- `weasel_sqlserver_testing_database` - SQL Server connection

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
- `ci-build-postgres.yml` - PostgreSQL CI
- `ci-build-mssql.yml` - SQL Server CI
- `publish_nuget.yml` - NuGet publishing

## Version

Defined in `Directory.Build.props`
