# Installation

## Install a Provider Package

Pick the package for your database and add it with the .NET CLI. Each provider package automatically includes `Weasel.Core`, so there is no need to install it separately.

**PostgreSQL:**

```bash
dotnet add package Weasel.Postgresql
```

**SQL Server:**

```bash
dotnet add package Weasel.SqlServer
```

**Oracle:**

```bash
dotnet add package Weasel.Oracle
```

**MySQL:**

```bash
dotnet add package Weasel.MySql
```

**SQLite:**

```bash
dotnet add package Weasel.Sqlite
```

**EF Core Integration:**

```bash
dotnet add package Weasel.EntityFrameworkCore
```

## Supported .NET Versions

Weasel targets .NET 8.0, .NET 9.0, and .NET 10.0.

## Driver Dependencies

Each provider package brings in the appropriate ADO.NET driver as a transitive dependency:

| Package              | Driver                            |
|----------------------|-----------------------------------|
| `Weasel.Postgresql`  | Npgsql                            |
| `Weasel.SqlServer`   | Microsoft.Data.SqlClient          |
| `Weasel.Oracle`      | Oracle.ManagedDataAccess.Core     |
| `Weasel.MySql`       | MySqlConnector                    |
| `Weasel.Sqlite`      | Microsoft.Data.Sqlite             |

You do not need to install these drivers separately unless you need a specific version.
