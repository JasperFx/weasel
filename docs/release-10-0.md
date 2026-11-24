# Upgrading to 10.0

Weasel 10.0 is the Weasel release in the Critter Stack .NET 11 release train, alongside JasperFx 3, Marten 10,
Polecat 6, Fisher 2, Wolverine 7 and CritterWatch 2. It targets `net10.0` and `net11.0`, moves to the new
driver majors, and takes in storage code that Marten, Polecat and Fisher each used to carry a copy of. This
page covers 10.0.0, and the end of it lists what changed since the release candidate.

**Read the [migration guide](/migration-guide-10) before you upgrade.** This page is the summary; the guide
has the details and what, if anything, you need to change.

## Targets

Every package targets `net10.0;net11.0`. `net9.0` is gone, so an application still on `net9.0` stays on the
9.x line. The `net11.0` build is compiled with runtime-async.

## Dependencies

| Package | `net10.0` | `net11.0` |
| --- | --- | --- |
| `JasperFx`, `JasperFx.Events` | 3.15.0 | 3.15.0 |
| `Npgsql`, `Npgsql.NetTopologySuite` | 11.0.0 | 11.0.0 |
| `Microsoft.Data.SqlClient` | 7.1.2 | 7.1.2 |
| `MySqlConnector` | 3.0.0 | 3.0.0 |
| `Oracle.ManagedDataAccess.Core` | 23.26.401 | 23.26.401 |
| `FirebirdSql.Data.FirebirdClient` | 10.3.4 | 10.3.4 |
| `Microsoft.Data.Sqlite` | 10.0.15 | 11.0.0 |
| `SQLitePCLRaw.bundle_e_sqlite3` | 3.0.5 | 3.0.5 |
| `Microsoft.EntityFrameworkCore.Relational` | 10.0.0 | 11.0.0 |

Npgsql 11 is on both frameworks, and it removed multiplexing, so take `Multiplexing=true` out of your
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
  a tenth of the time ([weasel#905](https://github.com/JasperFx/weasel/issues/905)), and a catalog read that
  is picked as a deadlock victim is read again rather than failing the migration
  ([weasel#900](https://github.com/JasperFx/weasel/issues/900)).
- **Partitioned tables**: a PostgreSQL partitioning rebuild locks the table and runs in a transaction, and the
  managed partition sets on both PostgreSQL and SQL Server handle tables that foreign keys reference
  ([weasel#890](https://github.com/JasperFx/weasel/issues/890),
  [weasel#896](https://github.com/JasperFx/weasel/issues/896)). On SQL Server, `ManagedTenantPartitions` can
  give its `SPLIT RANGE` a shorter timeout of its own
  ([weasel#877](https://github.com/JasperFx/weasel/issues/877)).
- **Provisioning timeouts**: `CREATE DATABASE` and extension installs get a two minute timeout of their own
  ([weasel#875](https://github.com/JasperFx/weasel/issues/875)).
- **Master table tenancy** evicts tenants another node removed or disabled, tells a disabled tenant from an
  unknown one ([weasel#809](https://github.com/JasperFx/weasel/issues/809)), and can resolve a tenant
  synchronously.
- **`Weasel.Storage`** gains the closed-shape document storage family
  ([weasel#740](https://github.com/JasperFx/weasel/issues/740),
  [weasel#811](https://github.com/JasperFx/weasel/issues/811)) and the rest of the storage code Marten,
  Polecat and Fisher share: the operation batch executor
  ([weasel#810](https://github.com/JasperFx/weasel/issues/810)), event row readers and appenders, union
  member SQL, and document metadata.
- **Asynchronous Hi-Lo ids** (`ISequence.NextLongAsync`), and sequential `Guid` ids from JasperFx's
  `SequentialGuid` instead of `Guid.CreateVersion7()`.
- **`RunSql<T>` is obsolete.** Use `RunSqlAsync<T>`.

## Also in this release

- `Weasel.Sqlite` stays on the `e_sqlite3` bundle ([weasel#739](https://github.com/JasperFx/weasel/issues/739)),
  and its `Microsoft.Data.Sqlite` floor fixes a pool race that could share one native handle between two
  connections ([weasel#857](https://github.com/JasperFx/weasel/issues/857)).
- `PostgresqlMigrator.RunsInOneTransaction` is public, for a custom `TableDelta` that writes its own
  transaction ([weasel#888](https://github.com/JasperFx/weasel/issues/888)).
- `EventLoaderBase` reports an event whose body will not deserialize on `EventPage.Unreadable` when the
  request asks it to park them ([weasel#901](https://github.com/JasperFx/weasel/issues/901)).
- Parameter-type lookups no longer allocate on a cache hit
  ([weasel#804](https://github.com/JasperFx/weasel/issues/804)).
- The Nuke build host targets `net10.0`, and `./build.sh LocalGate` runs every CI suite locally.

## Since the release candidate

- Two PostgreSQL sessions applying a migration that creates the same table at the same moment both succeed.
  The one that loses the race used to fail with `23505` on `pg_type_typname_nsp_index`, `42P07` or `42710`. It now
  determines its migration again and applies whatever is still missing
  ([weasel#918](https://github.com/JasperFx/weasel/issues/918)). You're most likely to have hit this when two
  application nodes started together with `AutoCreate` on.
- SQL Server partition functions and schemes are named after the schema as well as the table and column,
  `pf_{schema}_{table}_{column}` ([weasel#920](https://github.com/JasperFx/weasel/issues/920)). Both are
  database-scoped, so the same table in two schemas of one database used to share them, and a `SPLIT` or
  `MERGE` for one schema's tenants or boundaries changed the other's. A table that's already partitioned keeps
  the function and scheme it's on, so nothing is rebuilt, and `SqlServerPartitionObjects.FindSharedAcrossSchemasAsync`
  tells you whether two of your schemas already share one. See
  [Partition Function and Scheme Names](/sqlserver/partitioning#partition-function-and-scheme-names).
- `JasperFx` and `JasperFx.Events` move from 3.12.1 to 3.15.0.
