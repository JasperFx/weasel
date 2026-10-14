# Upgrading to 9.45

Two fixes, both about migrations that did the wrong thing to an existing database:

- A `db-patch` drop file could destroy schemas the patch never touched.
- Adding or removing a table's primary key on an existing table either failed or silently did
  nothing, depending on the provider.

**Nothing public was removed and no signature changed**, but three behaviours did, so read
[What changes for you](#what-changes-for-you) if you assert on deltas, inspect `PrimaryKeyName`, or keep
generated drop files around.

## `db-patch` drop files leave untouched schemas alone

[#706](https://github.com/JasperFx/weasel/issues/706) /
[#708](https://github.com/JasperFx/weasel/pull/708).
`Migrator.WriteMigrationFileAsync`, which `db-patch` calls, started every patch file with a
`CREATE SCHEMA` and every drop file with a `drop schema if exists ... CASCADE` for **every** schema the
application knows about, not just the ones the patch changes. Running that drop file as a rollback
cascaded through schemas the patch never touched. In the report, a patch that added one table produced a
drop file that began with 18 cascading schema drops, the Marten event store schema among them.

Now:

- The patch file creates only the schemas of the objects it changes. That statement stays because SQL
  Server has no schema existence check of its own; it is idempotent.
- The drop file never drops a schema. It rolls back each object the patch touched, which is all a
  rollback should do. An empty schema left behind is harmless; a cascade is not.
- Firebird can write a drop file again. Its schema drop throws for any schema other than `PUBLIC`, so
  writing one used to fail.

## Adding and removing primary keys on existing tables

[#707](https://github.com/JasperFx/weasel/issues/707) /
[#708](https://github.com/JasperFx/weasel/pull/708).
Reported from Wolverine
([JasperFx/wolverine#4989](https://github.com/JasperFx/wolverine/issues/4989)). Its SQL Server queue
tables switch between a clustered primary key on `id` and a clustered `IDENTITY` column with a unique
index on `id`. Neither direction could be migrated on a table that held rows. Fixing that turned up the
same family of problems in the other providers.

| Provider | Before 9.45 |
|---|---|
| All | A table with no primary key still reported a default primary key name (`pkey_<table>_`), so a delta could try to drop a constraint that did not exist |
| SQL Server | An `IDENTITY` column could not be added to an existing table ("cannot be added to an existing table"), although SQL Server numbers the existing rows itself |
| SQL Server | Removing the primary key dropped it **after** creating the new indexes. That key is usually the clustered index, so a new clustered index failed with "Cannot create more than one clustered index" |
| SQL Server | A primary key could not be added over an existing nullable column ("Cannot define PRIMARY KEY constraint on nullable column") |
| SQL Server, PostgreSQL, Oracle | Removing the primary key wrote `add CONSTRAINT ... PRIMARY KEY ()` for the key the table no longer declares |
| SQL Server, PostgreSQL, Oracle | Adding a primary key to an existing table made the whole table delta a `Create`, which only writes `CREATE TABLE IF NOT EXISTS`, so the key was never added. Oracle could hit this all along |

All of these now migrate, in both directions, on tables that hold data, and the rollbacks reverse them.

## What changes for you

- **`PrimaryKeyName` is empty for a table without primary key columns**, unless you set a name
  explicitly. It used to return the provider's default name built from no columns.
- **Adding a primary key to a table that had none reports `PrimaryKeyDifference.Create`**, not `Update`,
  and the table delta as a whole reports `Update`. If you assert on these values, update the expected
  value. The migration now actually adds the key.
- **Generated drop files contain no schema drops.** If you relied on a drop file to remove a schema the
  patch created, drop it yourself once it is empty.
- **SQL Server's opt-in column drift check (`DetectColumnDrift`) compares effective nullability**: a
  primary key column counts as `NOT NULL` instead of being skipped. A column that is about to become part
  of a primary key, or that leaves one, can now report drift.

## New public API

- `Weasel.Core.SchemaMigration.ChangedSchemas`: the schemas of the objects a migration actually changes.
  `Schemas` still lists every schema in the migration.

## 9.45.1

One security fix: [#710](https://github.com/JasperFx/weasel/pull/710), for
[GHSA-846f-f764-76mx](https://github.com/advisories/GHSA-846f-f764-76mx) / CVE-2026-84008 (high, CVSS
7.5). `SignedCms.CheckSignature` in `System.Security.Cryptography.Pkcs` can reparse attacker-controlled
X.509 data with quadratic CPU cost.

Weasel never calls that API itself, but two of its packages brought a vulnerable version of the package
in through their database drivers, and any application referencing them inherited it:

| Package | Pulled in by | Pkcs version before 9.45.1 |
|---|---|---|
| `Weasel.SqlServer` | Microsoft.Data.SqlClient 6.1.3 | 9.0.4 |
| `Weasel.Oracle` | Oracle.ManagedDataAccess.Core 23.7.0 | 8.0.0 |

Upgrading the driver alone does not help. The newest SqlClient, 7.1.1, still depends on Pkcs 9.0.18.
So both packages now reference `System.Security.Cryptography.Pkcs` directly, at **10.0.13** on net10.0
and **9.0.21** on net9.0, the first patched version on each line. Upgrading to 9.45.1 is enough to clear
`NU1903` for this advisory; no code changes.

If your application references `Microsoft.Data.SqlClient` or `Oracle.ManagedDataAccess.Core` directly
as well, NuGet already resolves the higher Pkcs version, so nothing else is needed.
