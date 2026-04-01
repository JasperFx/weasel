# Introduction

Weasel is a set of .NET libraries for database schema management and ADO.NET helpers. It gives you a programmatic API for defining database objects like tables, indexes, foreign keys, sequences, and functions, then handles the heavy lifting of detecting differences between your code-defined schema and the actual database, generating DDL, and applying migrations.

## Origin and the Critter Stack

Weasel was extracted from the [Marten](https://martendb.io) project during its V4 development cycle. What started as Marten's internal schema management layer is now a standalone, general-purpose library and a foundational part of the **Critter Stack** -- the family of .NET libraries that includes [Marten](https://martendb.io), [Wolverine](https://wolverinefx.net), and Polecat.

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

## NuGet Packages

| Package                        | Description                                      |
|--------------------------------|--------------------------------------------------|
| `Weasel.Core`                  | Core abstractions and migration infrastructure   |
| `Weasel.Postgresql`            | PostgreSQL provider (Npgsql)                     |
| `Weasel.SqlServer`             | SQL Server provider (Microsoft.Data.SqlClient)   |
| `Weasel.Oracle`                | Oracle provider (Oracle.ManagedDataAccess.Core)  |
| `Weasel.MySql`                 | MySQL provider                                   |
| `Weasel.Sqlite`                | SQLite provider (Microsoft.Data.Sqlite)          |
| `Weasel.EntityFrameworkCore`   | EF Core integration for Weasel migrations        |

All provider packages automatically reference `Weasel.Core`, so you only need to install the provider for your database.

## Next Steps

- [Installation](/guide/installation) -- Add Weasel to your project
- [Quick Start](/guide/quickstart) -- Define and migrate your first table in minutes
