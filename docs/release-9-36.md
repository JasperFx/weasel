# Upgrading to 9.36

9.36.0 is a drift release. Most of it is one shape of defect found five times over: a model
declares something in a spelling the server rewrites when it stores it, so the column or index
never matches itself, every migration re-issues DDL that changes nothing, and the delta never
settles. MySQL type synonyms, Oracle ANSI type names, Oracle and MySQL index key direction, Oracle
index tablespaces and SQL Server `CAST` all did this.

It also adds `IAdvisoryLock.FindHolderAsync` on PostgreSQL and SQL Server, so a node can finally ask
who holds a distribution lock it does not hold itself.

::: tip Coming from 9.35?
Three things to know.

**Oracle.** A table with a string column default could not be created at all under the default
`CreateIfNotExists` style — the quote ended the `EXECUTE IMMEDIATE` literal. If that bit you, it is
fixed.

**Anything that settled on 9.35 still settles.** Every drift fix here was checked in both
directions: the spellings that already reconciled still reconcile, and a genuinely different type,
length or direction is still reported as drift. No settled table acquires a new `ALTER`.

**One migration that used to fail now succeeds, and one that used to succeed now does more.**
Changing a computed column's definition drops and recreates the indexes and foreign keys on it.
On SQL Server that migration used to fail outright; on PostgreSQL it used to succeed while silently
dropping the index.
:::

## JasperFx 2.76.0 is the new floor

`JasperFx` and `JasperFx.Events` move from 2.74.0 to 2.76.0, which is the first release carrying
`IAdvisoryLock.FindHolderAsync` and the `AdvisoryLockHolder` record. Nothing else in Weasel needed a
change for it.

## Who holds this advisory lock?

[#650](https://github.com/JasperFx/weasel/issues/650).

`IAdvisoryLock` could acquire a distribution lock but not say who holds it. `HasLock(lockId)`
answers only "does *this* node", so no node — and no monitoring tool — could learn which node owns a
lock set it does not hold.

That is the diagnostic an operator needs when a projection agent stops with
`ProgressionProgressOutOfOrderException`, which means two processes believe they own the same shard.
Under Wolverine-managed distribution the assignment table answers "who owns this". Under the store's
own lock-based distribution — Marten HotCold, Polecat — the lock *is* the authority, and nothing
exposed its holder.

Both implementations live in Weasel, and both are now real:

```csharp
var holder = await advisoryLock.FindHolderAsync(lockId, token);

if (holder is null)
{
    // nothing holds it
}
else if (holder.IsCurrentNode == false)
{
    logger.LogWarning("Shard {Id} is held by session {Session} on {Client} since {Since}",
        holder.LockId, holder.SessionId, holder.ClientAddress, holder.HeldSince);
}
```

`null` means unheld. Everything past `LockId` is filled in where the primitive exposes it and left
null where it does not — null means "not known", never "empty".

`HeldSince` is the holder **session's** start (`pg_stat_activity.backend_start`,
`sys.dm_exec_sessions.login_time`), which is the earliest the lock can have been taken. Neither
server records when a lock was actually acquired.

The SQL Server read uses `sys.dm_tran_locks` and `sys.dm_exec_sessions`, so the connection needs
`VIEW SERVER STATE`. Without it the call throws rather than reporting the lock as unheld —
deliberately, because "nothing holds this" is exactly the reassuring conclusion an operator chasing
a double-runner must not be handed by accident.

## ⚠️ Oracle: a string column default made the table uncreatable

[#643](https://github.com/JasperFx/weasel/issues/643).

Under the default `CreationStyle.CreateIfNotExists` an Oracle table is created from inside a PL/SQL
guard block, as the text of an `EXECUTE IMMEDIATE '…'` literal. The column declarations went into
that literal unescaped, so any quote in them ended it early. A string default was enough:

```csharp
table.AddColumn("is_enabled", "VARCHAR2(1)").DefaultValueByString("0").NotNull();
await table.MigrateAsync(conn);
// ORA-06550 / PLS-00103: Encountered the symbol "0" ...
```

`CreateAsync`, `MigrateAsync` and `ApplyAllAsync` all failed; `DropThenCreate` worked, because it
runs `CREATE TABLE` as its own statement. The quotes are now doubled inside the guard block. DDL
with no quote in it is byte-for-byte unchanged.

Note that `DefaultValueByString` still does not escape its argument on any of the five providers.
That is a cross-provider change and is not in this release.

## Columns that never matched themselves

### Oracle stores a different type name than you declared

[#649](https://github.com/JasperFx/weasel/issues/649).

`ALL_TAB_COLUMNS` reports the name Oracle rewrote to, so a model using the declared spelling drifted
forever:

| Declared | Stored |
|---|---|
| `NUMERIC`, `DECIMAL`, `DEC`, `INTEGER`, `INT`, `SMALLINT` | `NUMBER` |
| `REAL` | `FLOAT` |
| `VARCHAR`, `CHARACTER VARYING` | `VARCHAR2` |
| `NATIONAL CHAR`, `NATIONAL CHARACTER VARYING` | `NCHAR`, `NVARCHAR2` |

Two more cases came with it. The catalog reports an interval as `INTERVAL DAY(2) TO SECOND(6)`,
which the reader cut to `INTERVAL DAY` — so **every `TimeSpan` column drifted**. And character
lengths were read from `DATA_LENGTH`, which is in bytes, so `VARCHAR2(100 CHAR)` read back as 400
and `NVARCHAR2(100)` as 200.

Both sides are now reduced to the name the catalog reports, and the reader uses `CHAR_LENGTH` /
`CHAR_USED`. A model length matching either the character or the byte count is accepted, so nothing
that reconciled before stops reconciling.

Still drifting, deliberately: `DOUBLE PRECISION` against the `FLOAT(126)` Oracle creates from it,
and `TIMESTAMP(n) WITH [LOCAL] TIME ZONE` against a plain `TIMESTAMP`. Changing either would emit an
`ALTER` on a populated column, which Oracle refuses with ORA-01439.

### MySQL rewrites a type synonym

[#646](https://github.com/JasperFx/weasel/issues/646).

`information_schema.COLUMNS.COLUMN_TYPE` reports the rewritten spelling, and the comparison only
stripped parentheses:

```csharp
table.AddColumn("is_durable", "BOOLEAN").NotNull();      // stored as tinyint(1)
table.AddColumn("amount", "NUMERIC(13,4)");              // stored as decimal
await table.CreateAsync(conn);
(await table.FindDeltaAsync(conn)).Difference;           // Update, forever
```

`INTEGER` → `int`, `BOOLEAN` → `tinyint(1)`, `NUMERIC`/`DEC`/`FIXED` → `decimal`,
`DOUBLE PRECISION`/`REAL`/`FLOAT(53)` → `double`, `CHARACTER VARYING`/`NATIONAL CHAR` →
`varchar`/`char`, and on 8.0 `INT(10) UNSIGNED` → `int unsigned`. All of them now fold to what the
catalog reports.

Two behaviour notes. `UNSIGNED` is compared whether or not the type has arguments, so a
signed/unsigned mismatch that a display width used to hide now shows as drift — correctly. And words
the catalog never reports after an argument-less type (`TEXT CHARACTER SET ascii`,
`INT AUTO_INCREMENT`) stop drifting.

`MySqlProvider.ConvertSynonyms` is untouched. It folds `TEXT` onto `VARCHAR(255)` and `REAL` onto
`FLOAT`, neither of which is what MySQL does, so it could not be wired in as is.

### SQL Server stores `CONVERT`, not `CAST`

[#637](https://github.com/JasperFx/weasel/issues/637).

A computed column declared with `CAST` could never reconcile:

| Declared | Stored |
|---|---|
| `CAST(JSON_VALUE(data, '$.customerId') AS uniqueidentifier)` | `(CONVERT([uniqueidentifier],json_value([data],'$.customerId')))` |

The two differ in keyword *and* argument order, which is past what stripping quoting and parentheses
can reconcile. `CAST(x AS t)` is now normalised to `CONVERT(t, x)` before the shared
canonicalization, for SQL Server only.

**This one was worse than churn.** A computed definition cannot be altered in place, so the delta
correctly emitted `DROP COLUMN` + `ADD` — and that `DROP` fails as soon as an index or foreign key
depends on the column. So a CAST-declared computed column carrying either did not merely re-run DDL
forever; the migration failed permanently, and the message named a dependency rather than the
expression that actually differed.

## Index key direction, per column

[#645](https://github.com/JasperFx/weasel/issues/645) (Oracle),
[#641](https://github.com/JasperFx/weasel/issues/641) (MySQL).

`IndexDefinition.SortOrder` can only say "descending" for the whole index, and neither reader could
report anything finer. MySQL never selected `STATISTICS.COLLATION` at all, so every index read back
ascending and a `SortOrder.Desc` model rebuilt its index on every migration. Oracle collapsed any
descending column to `SortOrder.Desc`, so `(a DESC, b)` compared equal to `(a, b DESC)` and that
difference was never reported.

Both now read direction per column into a new `DescendingColumns`, matching SQL Server's
[#513](https://github.com/JasperFx/weasel/issues/513):

```csharp
var index = new IndexDefinition("idx_qrtz_t_nft_st")
{
    Columns = ["sched_name", "trigger_state", "next_fire_time", "priority", "misfire_instr"]
};
index.DescendingColumns.Add("priority");
// ... next_fire_time, priority DESC, misfire_instr)
```

`SortOrder.Desc` keeps its existing meaning on both, so an index declared with it is not rebuilt.

Two things to know. On Oracle, a `SortOrder.Desc` model against an index whose descending columns are
not exactly the last one used to match and is now drift — an index Weasel created is never in that
state. And MySQL 5.7 parses `DESC` and ignores it, so a descending declaration still drifts there;
CI runs 8.0.

## Oracle: an index that names a tablespace

[#644](https://github.com/JasperFx/weasel/issues/644).

The reader never selected `all_indexes.tablespace_name`, so an index declaring `Tablespace` never
matched the index it had just created, and was dropped and recreated on every run. It is read back
now. The comparison only applies when the expected index names a tablespace — every index in the
catalog has one, so comparing unconditionally would turn every index that names none into drift.

## MySQL: `IgnoreIndex` is honoured

[#642](https://github.com/JasperFx/weasel/issues/642).

`TableBase.IgnoreIndex` is Weasel.Core API and the PostgreSQL, SQLite and SQL Server deltas all
honour it. The MySQL `TableDelta` never read `IgnoredIndexes`, so an ignored index was reported as an
extra and the generated patch **dropped it**.

Oracle's `TableDelta` still does not consult `IgnoredIndexes`. That is not fixed here.

## MySQL: a least-privilege user can migrate its own database

[#647](https://github.com/JasperFx/weasel/issues/647).

On MySQL a schema is a database, so every migration opened with
`CREATE DATABASE IF NOT EXISTS` for each schema the delta mentioned. MySQL checks the `CREATE`
privilege *before* it evaluates `IF NOT EXISTS`, so a user granted `ALTER, INDEX, …` on its own
database but not `CREATE` was refused for a database that already existed — on the first statement,
so nothing was applied:

```
InsufficientDatabasePrivilegeException: ... Access denied for user 'app_user'@'%' to database 'app' (1044)
```

`information_schema.SCHEMATA` is now read once and `CREATE DATABASE` is emitted only for the
databases that are missing. This is the MySQL twin of
[#495](https://github.com/JasperFx/weasel/issues/495); PostgreSQL and SQL Server already guarded the
same way. Rendered scripts are unchanged.

::: warning The MySQL default schema is still the literal `public`
`MySqlProvider`'s default schema name is `"public"`, so `new Table("users")` targets a database
called `public` — which the migration never creates, because `SchemaMigration.Schemas` filters it
out. `CREATE TABLE` then fails with 1142 or 1049 and introspection finds nothing.

Name the connection's database explicitly:

```csharp
new MySqlObjectName(new MySqlConnectionStringBuilder(connectionString).Database, "users")
```

Resolving the default would touch every MySQL object type and move the tables of anyone who created
a `public` database as a workaround, so 9.36 documents it rather than changing it. See
[Schemas are databases](/mysql/#schemas-are-databases).
:::

## SQLite

### A rebuild keeps what an add-only table does not declare

[#639](https://github.com/JasperFx/weasel/issues/639),
[#648](https://github.com/JasperFx/weasel/issues/648).

`AddOnlyMigrations` ([#629](https://github.com/JasperFx/weasel/issues/629)) keeps undeclared columns,
indexes and foreign keys out of the delta, so no `ALTER` drops them. But SQLite applies a column-type,
primary-key or foreign-key change by **rebuilding the table**, and the rebuild built the new table
from the model alone. So on an add-only table every rebuild dropped every undeclared object, and a
column's rows with it — while the log reported the drops as withheld.

Since `MapToTable` sets `AddOnlyMigrations` on every EF-derived table, this was the default for an
EF Core model on SQLite.

The rebuild now keeps what the model does not declare, taken from what SQLite stored:

| Undeclared | Kept as |
|---|---|
| column | its definition from the stored `CREATE TABLE`, verbatim, with its rows |
| index | its stored `CREATE INDEX` |
| `UNIQUE` / `CHECK` table constraint | verbatim |
| foreign key | as introspected, unless the model already declares the same key |

An `IgnoredIndexes` index is kept on every table, not just an add-only one. An undeclared column that
is part of the primary key is refused with `SchemaMigrationException` before any statement runs.

**Rolling back a rebuild keeps the rows too.** `WriteAllRollbacks` answered every `Invalid` delta
with a `DROP` and the previous `CREATE`, so every rollback of a rebuild emptied the table — the
`db-patch` `.drop` file, `RollbackAllAsync` and the EF down migration alike. It now runs the reverse
rebuild. If the old shape cannot be refilled, the rollback fails atomically instead of succeeding
empty.

### `PreserveIdentifierCase` stopped matching anything

[#640](https://github.com/JasperFx/weasel/issues/640).

With `PreserveIdentifierCase = true` a SQLite table never matched the database, not even one it had
just created. The catalog read folds column names to lowercase and SQLite's `ItemDelta` paired names
case-sensitively, so every column was both Missing and Extra — and every apply renamed each column to
itself or rebuilt the table:

```
ALTER TABLE pc_orders RENAME COLUMN id TO Id;   -- every single migration
```

Names now pair with `OrdinalIgnoreCase`, as every other provider already did. SQLite identifiers are
case-insensitive. As on the other providers, two foreign key names differing only in case now make
the delta throw.

`MapToTable` sets `PreserveIdentifierCase` on every EF-derived table, so this was the default for an
EF Core model on SQLite.

### Delete-all against a schema with no `AUTOINCREMENT` table

[#546](https://github.com/JasperFx/weasel/issues/546), open since 9.29.0.

`sqlite_sequence` exists in a database only once something in it has been declared `AUTOINCREMENT`,
and SQLite resolves table names when it *prepares* a statement — so the reset failed against a schema
that had none, and no `WHERE` guard could prevent it:

```
SqliteException: SQLite Error 1: 'no such table: main.sqlite_sequence'
```

Because `Microsoft.Data.Sqlite` prepares and steps one statement at a time, the table deletes ahead
of it had already run: **the operation half applied and then threw.**

`Migrator` gains a connection-aware overload that defaults to the existing behaviour:

```csharp
public virtual Task<string> GenerateDeleteAllSqlAsync(
    DbConnection conn, IReadOnlyList<DbObjectName> tables,
    bool resetIdentity = true, CancellationToken ct = default);
```

`SqliteMigrator` overrides it and asks each schema whether it has a sequence, emitting a reset only
for those that do. No other provider is affected, and `GenerateDeleteAllSql` keeps its behaviour for
anyone calling it directly.

`DatabaseCleaner` now generates its SQL per call rather than memoizing it, because the answer follows
the database rather than the model — the sequence appears the first time anything is declared
`AUTOINCREMENT`. The reflection over the EF model, which is the expensive part, is still cached.

## ⚠️ A computed column's dependants survive a definition change

[#638](https://github.com/JasperFx/weasel/issues/638). SQL Server **and** PostgreSQL.

A computed definition cannot be altered in place, so the delta drops and re-adds the column. It
dropped the indexes that were Extra or Different first — but an index that *matches* the model is in
neither set, and no foreign key was dropped at any point.

On **SQL Server** the server refuses:

```
The index 'ix_cc' is dependent on column 'cc'.
Msg 4922 ... ALTER TABLE DROP COLUMN cc failed because one or more objects access this column.
```

On **PostgreSQL** it does not refuse — it succeeds and silently drops every index and constraint on
the column. Nothing recreated the matching index, so one `ApplyAll` left the schema not matching the
model and the index was simply gone until something ran again.

Both now drop the dependants of a recomputed column before it and recreate them after. Indexes and
foreign keys that do not touch the column are untouched, and the pass is inert unless a computed
definition actually changed.

This is reachable from ordinary configuration changes: widening a declared type, a serializer
naming-policy change that retargets a JSON path, or switching a store to a native `json` column type.

## Contributing: one npm tree for the docs

[#582](https://github.com/JasperFx/weasel/issues/582).

The repository carried two npm dependency trees for one docs site, and Dependabot counted every
advisory in the shared subtree twice. The root `package.json` is now the only one;
`docs/package.json` is reduced to the `{"type": "module"}` marker that makes Node read
`docs/.vitepress/config.ts` as ESM, and its lockfile is gone.

`cd docs && npm run dev` no longer exists. Use the root scripts, which are what CI uses:

```bash
npm ci
npm run docs          # dev server on port 5050
npm run docs-build    # production build
```
