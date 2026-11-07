# Introduction

Weasel is a set of .NET libraries for database schema management and ADO.NET helpers. It gives you a programmatic API for defining database objects like tables, indexes, foreign keys, sequences, and functions, then handles the heavy lifting of detecting differences between your code-defined schema and the actual database, generating DDL, and applying migrations.

## Origin and the Critter Stack

Weasel was extracted from the [Marten](https://martendb.io) project during its V4 development cycle. What started as Marten's internal schema management layer is now a standalone, general-purpose library and a foundational part of the **Critter Stack** -- the family of .NET libraries that includes [Marten](https://martendb.io), [Wolverine](https://wolverinefx.net), [Polecat](https://polecat.jasperfx.net), and [Fisher](https://fisher.jasperfx.net). Every table those tools create, from a Marten document table to Wolverine's inbox and outbox to a Polecat event stream table, is defined as a Weasel schema object and migrated by Weasel. The [CritterWatch](https://critterwatch.jasperfx.net) console manages its own database the same way.

That is also why the `db-apply`, `db-assert`, `db-patch`, `db-dump` and `db-list` commands look the same in every one of those tools: they are Weasel's [command line tools](/cli/), and the databases they act on are the ones each library registers. The JasperFx [stateful resource](https://shared-libs.jasperfx.net/cli/resources.html) model sits one level up, so the same databases can be set up alongside message broker objects with `resources setup` or at application startup.

Weasel is developed and maintained by [JasperFx Software](https://jasperfx.net).

## Key Capabilities

- **Programmatic schema definitions** -- Define tables, columns, indexes, foreign keys, sequences, functions, and views in C# code
- **Automatic delta detection** -- Compare your expected schema against the actual database state and compute the minimal set of changes needed
- **DDL generation** -- Generate CREATE, ALTER, and DROP statements for any supported database engine
- **Schema migration** -- Apply detected changes automatically or review them before execution
- **CLI migration tools** -- Command-line interface for asserting schema validity, applying patches, and dumping DDL
- **EF Core integration** -- Use Weasel's migration infrastructure alongside Entity Framework Core

## Supported Databases

| Database   | Package                |
|------------|------------------------|
| PostgreSQL | `Weasel.Postgresql`    |
| SQL Server | `Weasel.SqlServer`     |
| Oracle     | `Weasel.Oracle`        |
| MySQL      | `Weasel.MySql`         |
| SQLite     | `Weasel.Sqlite`        |
| Firebird   | `Weasel.Firebird`      |

## NuGet Packages

| Package                        | Description                                      |
|--------------------------------|--------------------------------------------------|
| `Weasel.Core`                  | Core abstractions and migration infrastructure   |
| `Weasel.Postgresql`            | PostgreSQL provider (Npgsql)                     |
| `Weasel.SqlServer`             | SQL Server provider (Microsoft.Data.SqlClient)   |
| `Weasel.Oracle`                | Oracle provider (Oracle.ManagedDataAccess.Core)  |
| `Weasel.MySql`                 | MySQL provider                                   |
| `Weasel.Sqlite`                | SQLite provider (Microsoft.Data.Sqlite)          |
| `Weasel.Firebird`              | Firebird provider (FirebirdSql.Data.FirebirdClient) |
| `Weasel.EntityFrameworkCore`   | EF Core integration for Weasel migrations        |

All provider packages automatically reference `Weasel.Core`, so you only need to install the provider for your database.

## Next Steps

- [Installation](/guide/installation) -- Add Weasel to your project
- [Quick Start](/guide/quickstart) -- Define and migrate your first table in minutes
- [Choosing a Schema Management Approach](/guide/schema-management) -- Runtime migration, startup setup, deployment commands, or DBA-reviewed scripts
- [Command Line Tools](/cli/) -- `db-apply`, `db-assert`, `db-patch`, `db-dump`, `db-list`, and how they relate to `resources`
