# Upgrading to 9.26

9.26 is a bug-fix release with one new capability. Ten changes, eight of them fixes for schemas
that Weasel was getting wrong — usually **silently**, which is what makes several of these worth
reading even if nothing looks broken from where you are sitting.

::: warning Two behaviours change in ways you can observe
`DbObjectName.Schema` and `.Name` now hold the *undelimited* name, and whitespace inside a SQL
Server function body is now significant. Both are described below. Emitted DDL is unchanged in
every other respect.
:::

## The silent ones

These four produced no error. If any of them describes your schema, the database was not what
Weasel reported it to be.

### A delimited name meant the object was created once and never migrated again

[#499](https://github.com/JasperFx/weasel/issues/499). A schema or table name can reach the model
already delimited — `DbObjectName.Parse` splits a qualified name and keeps the parts as written, and
before 9.25 delimiting a name by hand was the only way to use one that needed it. Nothing
normalized it afterwards, and introspection binds that name against catalogs that hold the **bare**
one.

The delimited spelling never matched, so the object read as absent, and the delta was `Create` on
every run — written as `CREATE TABLE IF NOT EXISTS`, which succeeds against the table that is
already there and does nothing:

```
--- declared as "MixedCase".things ---
delta after adding a column = Create
added_later present in the database? NO
```

**Every change after the first was discarded, with no error, and `ApplyAllAsync` returned
normally.** On SQL Server it surfaced loudly instead — `There is already an object named 'x' in the
database` — because the `CREATE SCHEMA` guard missed the bracketed name the same way.

Names are now normalized on the way into the model, which is the change to
`DbObjectName.Schema`/`.Name` flagged above. If you compare either property against a literal that
includes quotes or brackets, that comparison changes meaning. Names that arrive bare are
byte-identical, and so is all emitted DDL.

### No index was read in any schema named `pg…`

[#504](https://github.com/JasperFx/weasel/issues/504). The index introspection query carried
`NOT nspname LIKE 'pg%'` beside the `nspname = :schema` it was already filtered by. It could never
have excluded anything — except a user schema starting with those two letters, which it excluded
completely.

`pgcontrol`, `pgqueues`, `pgdata`: no index was read back at all. The primary key still arrived, so
the table looked correct and only its non-PK indexes were missing. Every declared index was then
reported `Missing` rather than `Different`, so the patch was a bare `create index` with no drop —
`42P07` on the second startup, which aborted everything ordered after it. The schema never
converged, so `db-assert` could not pass either.

### An invalid index reported no drift

[#503](https://github.com/JasperFx/weasel/issues/503). Weasel did not read `pg_index.indisvalid`,
and `pg_get_indexdef` renders an invalid index exactly like a valid one.

An invalid index is **ignored by the planner**. The object exists, `\d` shows it, Weasel said the
schema matched configuration, and every query meant to use it silently did a sequential scan.
PostgreSQL leaves an index in that state whenever `CREATE INDEX CONCURRENTLY` fails partway. Such
an index is now reported as drift and rebuilt.

### A SQL Server column with a default could never be dropped

[#505](https://github.com/JasperFx/weasel/issues/505). SQL Server refuses `DROP COLUMN` while a
default constraint references the column, so a column declared with `DefaultValue(...)` could be
added by a migration but never removed by one — the patch was generated, it just always failed.
Weasel now finds the constraint (SQL Server names it itself, so it has to be looked up) and drops it
first.

## Migrations that failed outright

### More than about a thousand tables on SQL Server

[#496](https://github.com/JasperFx/weasel/pull/496), and
[#497](https://github.com/JasperFx/weasel/pull/497) for the public `MigrateAsync` extensions.
Introspection put every object's query into one command; SQL Server refuses a request carrying more
than 2100 parameters, and a table's query binds two. Past roughly 1050 tables the migration failed
before any comparison happened. Queries are now batched under the dialect's limit.

### A least-privilege role could not migrate into its own schema

[#495](https://github.com/JasperFx/weasel/pull/495) and
[#498](https://github.com/JasperFx/weasel/pull/498). PostgreSQL checks `CREATE` on the *database*
before it evaluates `CREATE SCHEMA`'s own `IF NOT EXISTS`, so a role granted `USAGE, CREATE` on one
schema and nothing else was refused with `42501 permission denied for database` — on a schema that
had been there for months. Since that statement opens the script, the whole migration aborted with
nothing applied. There is now an existence pre-check, which is what `SqlServerMigrator` has always
done.

### A name containing a dot could not be parsed

[#501](https://github.com/JasperFx/weasel/issues/501). Qualified names were split on every `.`, so a
legal delimited name containing one — `"my.schema".things`, `[my.table]`, or the
`PK_dbo.__MigrationHistory` that EF6 gives its own history table — threw at parse time. The split is
now delimiter-aware.

Providers expose their identifier rules through a new `IDatabaseProvider.Rules` property. It is
virtual and defaults to `null`, so a provider implemented outside Weasel keeps compiling and keeps
its current splitting.

## New: a non-blocking index on a partitioned table

[#494](https://github.com/JasperFx/weasel/issues/494). `IndexDefinition.IsConcurrent` emits
`CREATE INDEX CONCURRENTLY`, which PostgreSQL refuses on a partitioned parent — so adding an index
to a partitioned table meant an `ACCESS EXCLUSIVE` lock for the whole build, a write outage rather
than a migration.

Weasel now emits the supported three-step sequence: the parent index `ON ONLY`, each partition's
index built concurrently, and an `ALTER INDEX ... ATTACH PARTITION` for each. Nothing changes unless
an index is both concurrent *and* on a partitioned table.

## SQL Server function bodies: whitespace is now significant

[#496](https://github.com/JasperFx/weasel/pull/496). `CanonicizeSql` collapsed all whitespace and
rewrote a list of PostgreSQL-isms that could never appear in T-SQL. It now normalizes line endings —
the same source file is CRLF on one machine and LF on another, and both apply to one database — and
the `CREATE [OR ALTER] FUNCTION` preamble, which is the only thing `sys.sql_modules` does not return
as written.

**Reformatting a declared function body is now a change.** It costs one drop-and-recreate, after
which it stays clean. Nothing is lost; the comparison simply matches what the catalog actually
stores.
