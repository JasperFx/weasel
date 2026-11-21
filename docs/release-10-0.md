# Upgrading to 10.0

Weasel 10.0 is the Weasel release in the Critter Stack .NET 11 release train, alongside JasperFx 3, Marten 10,
Polecat 6, Fisher 2, Wolverine 7 and CritterWatch 2. It targets `net10.0` and `net11.0`, moves to the new
driver majors, and takes in storage code that Marten, Polecat and Fisher each used to carry a copy of. This
page covers 10.0.0 and its release candidates.

**Read the [migration guide](/migration-guide-10) before you upgrade.** This page is the summary; the guide
has the details and what, if anything, you need to change.

## Targets

Every package targets `net10.0;net11.0`. `net9.0` is gone, so an application still on `net9.0` stays on the
9.x line. The `net11.0` build is compiled with runtime-async.

## Dependencies

| Package | `net10.0` | `net11.0` |
| --- | --- | --- |
| `JasperFx`, `JasperFx.Events` | 3.12.1 | 3.12.1 |
| `Npgsql`, `Npgsql.NetTopologySuite` | 11.0.0 | 11.0.0 |
| `Microsoft.Data.SqlClient` | 7.1.2 | 7.1.2 |
| `MySqlConnector` | 3.0.0 | 3.0.0 |
| `Oracle.ManagedDataAccess.Core` | 23.26.401 | 23.26.401 |
| `FirebirdSql.Data.FirebirdClient` | 10.3.4 | 10.3.4 |
| `Microsoft.Data.Sqlite` | 10.0.15 | 11.0.0 |
| `SQLitePCLRaw.bundle_e_sqlite3` | 3.0.5 | 3.0.5 |
| `Microsoft.EntityFrameworkCore.Relational` | 10.0.0 | 11.0.0 |

Npgsql 11 is on both frameworks, and it removed multiplexing: take `Multiplexing=true` out of your
connection strings. The other packages follow the target framework, so `net10.0` stays on the .NET 10
releases and `net11.0` moves to the .NET 11 ones.

`Weasel.EntityFrameworkCore` is tested against the SQL Server, Npgsql and Firebird EF Core providers on both
frameworks. Oracle's EF Core provider has no EF Core 11 release yet, so Oracle with EF Core works on `net10.0`
only. Pomelo's MySQL provider has no EF Core 10 release, so the MySQL half of the EF Core tests is compiled
out. `Weasel.MySql` itself is unaffected.

`Patched.DistributedLock.Postgres` and the `System.Security.Cryptography.Pkcs` override are gone.

## The headline changes

- **`AdvisoryLock` takes its own advisory locks.** DistributedLock does not work on Npgsql 11, so Weasel
  calls `pg_try_advisory_lock` and friends itself. Same API, same behaviour
  ([weasel#908](https://github.com/JasperFx/weasel/issues/908)).
- **Schema migrations on SQL Server** move a table onto RANGE partitioning in place instead of dropping it,
  change key columns across foreign keys on other tables, fill NULLs before a column becomes NOT NULL, and
  read back foreign key, primary key and trigger state faithfully. SQL Server introspection compiles in about
  a tenth of the time ([weasel#905](https://github.com/JasperFx/weasel/issues/905)).
- **Partitioned PostgreSQL tables**: a partitioning rebuild locks the table and runs in a transaction, and
  managed partitions handle tables that foreign keys reference.
- **Provisioning timeouts**: `CREATE DATABASE` and extension installs get a two minute timeout of their own.
- **Master table tenancy** evicts tenants another node removed or disabled, tells a disabled tenant from an
  unknown one, and can resolve a tenant synchronously.
- **`Weasel.Storage`** gains the closed-shape document storage family and the rest of the storage code
  Marten, Polecat and Fisher share: the operation batch executor, event row readers and appenders, union
  member SQL, and document metadata.
- **Asynchronous Hi-Lo ids** (`ISequence.NextLongAsync`), and sequential `Guid` ids from JasperFx's
  `SequentialGuid` instead of `Guid.CreateVersion7()`.
- **`RunSql<T>` is obsolete.** Use `RunSqlAsync<T>`.

## Also in this release

- `Weasel.Sqlite` stays on the `e_sqlite3` bundle, and its `Microsoft.Data.Sqlite` floor fixes a pool race
  that could share one native handle between two connections
  ([weasel#857](https://github.com/JasperFx/weasel/issues/857)).
- The Nuke build host targets `net10.0`, and `./build.sh LocalGate` runs every CI suite locally.
