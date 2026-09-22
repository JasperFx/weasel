# gh-593: batch-safe and idempotent SQL Server migration scripts

Issue: [JasperFx/weasel#593](https://github.com/JasperFx/weasel/issues/593)
Branch: `gh-593/idempotent-sqlserver-migrations`
Plan: `docs/design/gh-593-idempotent-sqlserver-migrations-plan.md`

## Context

A Wolverine user ran `db-patch` against SQL Server and got a script that SQL Server rejects:
`CREATE INDEX ...` runs straight into `CREATE OR ALTER PROCEDURE ...` with nothing between
them, and T-SQL requires a procedure definition to be the only statement in its batch. Their
second ask was that the script be safe to run twice: schema creation is already guarded with
`IF NOT EXISTS`, but index and procedure creation are not.

Both asks live in `Weasel.SqlServer`. Neither reproduces per object, which is why no test caught
them:

- The runtime apply path, `SqlServerMigrator.executeDelta`
  (`src/Weasel.SqlServer/SqlServerMigrator.cs:113-141`), renders and executes one command per
  delta, so a procedure always lands in a batch of its own by accident.
- The file path concatenates every delta into one writer with nothing between them:
  `Migrator.WriteMigrationFileAsync` (`src/Weasel.Core/Migrator.cs:263-290`) writes the schema
  header and then `SchemaMigration.WriteAllUpdates`
  (`src/Weasel.Core/SchemaMigration.cs:405-414`). `db-dump` has the same shape.

This is the same "only fails in combination" trap that `CLAUDE.md` documents for unterminated
introspection queries.

The intended outcome: a generated SQL Server script (a) is valid when fed to `sqlcmd` or SSMS as
one file, (b) can be run a second time against the same database without error, and (c) still
executes through Weasel's own `SqlCommand` based apply path, which does not understand `GO`.

## Decisions fixed by the caller

These were settled before this document was written and are designed around, not reopened:

1. Procedures get `GO` batch separators, not the `EXEC sp_executesql` wrapper that `View`
   (`src/Weasel.SqlServer/Views/View.cs:42-50`), `Function` and `Trigger` use. Views, functions
   and triggers keep their wrapper; they are out of scope.
2. Because `GO` is not T-SQL, every place that executes rendered DDL through `SqlCommand` splits
   on `GO` lines first, with sqlcmd semantics, through one shared splitter.
3. Procedure DDL is always `CREATE OR ALTER PROCEDURE`, on create and on update.
4. Index and foreign key DDL is guarded at the emission sites; `IndexDefinition.ToDDL` stays
   unguarded because comparison uses it.
5. SQL Server 2016 SP1 or later is already assumed (`DROP CONSTRAINT IF EXISTS` is emitted today
   at `src/Weasel.SqlServer/Tables/ForeignKey.cs:150`), so `DROP INDEX IF EXISTS` and
   `CREATE OR ALTER` are acceptable.

## Current behaviour

Every anchor below was read on this branch at the time of writing.

| Emitted DDL | Guarded today | Where |
| --- | --- | --- |
| `CREATE SCHEMA` | yes, `IF NOT EXISTS (... sys.schemas ...)` | `src/Weasel.SqlServer/SqlServerMigrator.cs:245-254` |
| `CREATE TABLE` | yes, `IF OBJECT_ID('...') IS NULL BEGIN ... END` | `src/Weasel.SqlServer/Tables/Table.cs:187-190, 257-260` |
| `CREATE [UNIQUE] [CLUSTERED] INDEX` | no | `src/Weasel.SqlServer/Tables/IndexDefinition.cs:142-197` |
| `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY` | no | `src/Weasel.SqlServer/Tables/ForeignKey.cs:136-146` |
| `drop index ... on ...` | no | `src/Weasel.SqlServer/Tables/StringWriterExtensions.cs:23-26` |
| `CREATE PROCEDURE` (create path) | no, raw body | `src/Weasel.Core/StoredProcedureBase.cs:49-57` |
| `CREATE OR ALTER PROCEDURE` (update path) | by construction | `src/Weasel.SqlServer/Procedures/StoredProcedure.cs:56-63` |
| FK and PK drops | yes, `DROP CONSTRAINT IF EXISTS` | `src/Weasel.SqlServer/Tables/ForeignKey.cs:150` |

The sharpest defect: `Table.WriteCreateStatement` closes its `IF OBJECT_ID ... BEGIN ... END`
block at `Table.cs:257-260` and then emits foreign keys (`:266-275`) and indexes (`:278-282`)
outside it. A second run skips the table and fails on the first `CREATE INDEX`.

`StoredProcedure.WriteCreateOrAlterStatement` rewrites the body with two case-exact
`string.Replace` calls (`StoredProcedure.cs:58-60`). It misses `CREATE PROC`, mixed case, and a
body already written as `CREATE OR ALTER`.

The delta compares `expected.CanonicizeSql()` against `actual.CanonicizeSql()`
(`src/Weasel.SqlServer/Procedures/StoredProcedureDelta.cs:23-25`), where `Canonicize` collapses
runs of whitespace and trims lines (`src/Weasel.Core/StoredProcedureBase.cs:92-96`). Verified
against the local Docker SQL Server on 2026-09-21: a procedure created with
`CREATE OR ALTER PROCEDURE` is stored in `sys.sql_modules.definition` as `CREATE   PROCEDURE`
(the `OR ALTER` is blanked in place, the same behaviour `Canonicalization.cs:7-14` documents for
functions). So today a body authored as `CREATE PROCEDURE` compares clean, but a body authored as
`CREATE OR ALTER PROCEDURE` never does: expected canonicalises to `CREATE OR ALTER PROCEDURE`,
actual to `CREATE PROCEDURE`, and the delta reports `Update` on every run. Wolverine's bodies are
authored that way, which is the likeliest reason the reporter's script showed `CREATE OR ALTER`.

There is no `GO` handling anywhere in `Weasel.Core` or `Weasel.SqlServer`.

## Design

### 1. Stored procedure emission

`Weasel.SqlServer.Procedures.StoredProcedure` overrides `WriteCreateStatement` (currently
inherited raw from `StoredProcedureBase.cs:49-57`) and rewrites `WriteCreateOrAlterStatement` so
both paths emit the same text:

```
GO
CREATE OR ALTER PROCEDURE procs.uspDeleteIncomingEnvelopes
    ...
GO
```

Rules:

- `IsRemoved` short-circuits exactly as the base does (`StoredProcedureBase.cs:51-54`).
- The body is passed through one normaliser, `StoredProcedure.NormalizeCreateStatement(string)`
  (internal static, so it is unit-testable). It finds the first `CREATE [OR ALTER] PROC[EDURE]`
  token after any leading whitespace, `--` line comments or `/* */` block comments, case
  insensitive, and replaces just that token with `CREATE OR ALTER PROCEDURE`. Only the first
  occurrence is touched, so the words inside a later string literal are left alone. A body that
  already reads `CREATE OR ALTER PROCEDURE` comes back unchanged.
- The `GO` lines are written by the two `Write*` methods, not by the normaliser, so `BodyText()`
  and `CanonicizeSql()` never see them.
- `WriteDropStatement` (`StoredProcedure.cs:29-32`) is unchanged; `DROP PROCEDURE IF EXISTS` can
  share a batch.
- `StoredProcedureDelta.WriteUpdate` and `WriteRollback` (`StoredProcedureDelta.cs:33-63`) need
  no change: they already call the two methods above, and the rollback's
  `Actual!.WriteCreateStatement` picks up the override.

Why `GO` rather than `sp_executesql` for procedures: fixed by the caller. The practical reason is
readability of Wolverine's long procedure bodies in the generated file, which the string literal
wrapper destroys. The cost is decision 2: every executor has to learn to split.

### 2. Delta comparison for procedures

`StoredProcedureDelta.compare` (`StoredProcedureDelta.cs:11-31`) canonicalises both sides
through a new preamble normaliser before the existing `CanonicizeSql` comparison. The normaliser
maps `CREATE [OR ALTER] PROC[EDURE]` to `CREATE PROCEDURE` on both the expected body and the
catalog text. This is the procedure analogue of `Canonicalization.CreatePreamble`
(`src/Weasel.SqlServer/Canonicalization.cs:13-14`), which already does it for functions, and it
is what stops the always-`Update` loop described above. Keep it in `Canonicalization.cs` next to
the function regex.

`StoredProcedure.NormalizeCreateStatement` (emission) and the comparison normaliser are two
directions of the same regex: emission rewrites to the `OR ALTER` form, comparison rewrites to the
bare form. One regex, two replacement strings.

### 3. The batch splitter

`src/Weasel.SqlServer/SqlServerBatchSplitter.cs`, `public static class SqlServerBatchSplitter`
with `public static IReadOnlyList<string> Split(string sql)`.

Semantics match sqlcmd:

- A separator is a line matching `^\s*GO(?:\s+(\d+))?\s*;?\s*$` with `RegexOptions.Multiline |
  RegexOptions.IgnoreCase`. `go`, `GO;`, `GO 3` and indented `GO` are all separators.
- A line that is only `GO` is a separator regardless of context. sqlcmd does not parse string
  literals or comments, so neither does this; a body that needs a literal line reading `GO` is
  not supported and the docs say so.
- Batches that are empty or whitespace only are dropped.
- The optional count is accepted and ignored: the batch runs once. sqlcmd would repeat it, but
  nothing in Weasel emits a count and repeating DDL is never what a migration means.
- Line endings: CRLF and LF both work (the regex allows trailing `\r` through `\s*`).

The splitter is pure and provider-local. `Weasel.Core` gets one small seam so the one core
executor that renders provider text can use it:

```csharp
// Weasel.Core/Migrator.cs
public virtual IReadOnlyList<string> SplitIntoBatches(string sql) => [sql];
```

`SqlServerMigrator` overrides it to call the splitter. Other providers keep the default.

### 4. Every executor of rendered DDL

Found by grepping `ExecuteNonQueryAsync`, `RunSqlAsync` and `CreateCommand(` across
`Weasel.SqlServer`, `Weasel.Core`, `Weasel.EntityFrameworkCore` and the SQL Server test projects.
"Rendered DDL" means text produced by `WriteCreateStatement`, `WriteDropStatement`,
`WriteUpdate`, `WriteRollback` or `WriteAllUpdates`; hand-written SQL strings are not affected.

| Call site | What it executes | Change |
| --- | --- | --- |
| `SqlServerMigrator.executeCommand` (`SqlServerMigrator.cs:224-243`) | one delta's `WriteUpdate` text (`:126-130`), the deferred FK text (`:135-139`), schema creation (`:216-219`) | split, run each batch as its own `SqlCommand`; `logger.SchemaChange` per batch; on failure call `logger.OnFailure` with the failing batch and stop the remaining batches of that text |
| `SchemaObjectsExtensions.CreateAsync` (`SchemaObjectsExtensions.cs:37-43`) | `WriteCreateStatement` | split and run each batch |
| `SchemaObjectsExtensions.Drop` (`SchemaObjectsExtensions.cs:29-35`) | `WriteDropStatement` | same, for symmetry (drops never contain `GO` today) |
| `SchemaMigration.RollbackAllAsync` (`src/Weasel.Core/SchemaMigration.cs:533-541`) | `WriteAllRollbacks`, which includes `StoredProcedureDelta.WriteRollback` | use `rules.SplitIntoBatches` and run each batch |
| `IntegrationContext.CreateSchemaObjectInDatabase` and `DropSchemaObjectInDatabase` (`src/Weasel.SqlServer.Tests/IntegrationContext.cs:34-59`) | `WriteCreateStatement` / `WriteDropStatement` | delegate to the two extension methods above so the test helper cannot drift |

Paths that are already covered or do not need splitting:

- `DatabaseBase` apply paths (`src/Weasel.Core/Migrations/DatabaseBase.cs:412, 623`) and the EF
  bridge's `DbContextExtensions.cs:32` go through `Migrator.ApplyAllAsync`, which calls
  `executeDelta`.
- `Weasel.EntityFrameworkCore` renders `WriteAllUpdates` (`EfSnapshotDiffer.cs:90`) and
  `WriteCreateStatement` (`MigrationOperationTranslation.cs:425`, `EfSchemaSnapshot.cs:245`) into
  `migrationBuilder.Sql(@"...")` (`EfMigrationFileEmitter.cs:313`). EF Core's SQL Server
  migrations SQL generator splits `Sql()` on `GO` itself, so the emitted files are fine. The two
  executors in `EfMigrationGenerator.cs:319, 353` and `DatabaseCleaner.cs:93` run hand-written
  history table and delete SQL, not rendered DDL. No EF Core test executes rendered SQL Server
  DDL text directly (grep for `CREATE INDEX`, `ADD CONSTRAINT`, `IF OBJECT_ID` in
  `Weasel.EntityFrameworkCore.Tests` finds only hand-written cleanup SQL).
- `Weasel.Core/CommandExtensions.RunSqlAsync` (`:71-84`) is a generic helper for arbitrary SQL;
  its Weasel callers pass hand-written text. Left alone.
- `index_ddl_generation.cs:101` executes `index.ToDDL(table)` directly; `ToDDL` stays unguarded
  and has no `GO`, so it is unaffected.

### 5. Idempotent index and foreign key DDL

**Index create.** `IndexDefinition` gains
`public void WriteCreateStatement(Table parent, TextWriter writer)` which emits:

```
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_1' AND object_id = OBJECT_ID(N'dbo.people'))
    CREATE INDEX idx_1 ON dbo.people (column1);
```

The second line is `ToDDL(parent)` unchanged. The literals go through `SchemaUtils.EscapeLiteral`
and the table uses `parent.Identifier.QualifiedName`, the same spelling `Table.cs:187` already
passes to `OBJECT_ID`. `ToDDL(Table)` and `ToDDL(Table, bool)` keep their current output, so
`CanonicizeDdl` (`IndexDefinition.cs:299-316`) and every existing `ToDDL` assertion are
untouched.

Emission sites move to the new method:

- `Table.WriteCreateStatement`, `Table.cs:281`
- `TableDelta.WriteUpdate`, `TableDelta.cs:186` (missing) and `:189` (different, after the drop
  at `:149`)
- `TableDelta.rollbackIndexes`, `TableDelta.cs:348` and `:354`

**Index drop.** `StringWriterExtensions.WriteDropIndex` (`StringWriterExtensions.cs:23-26`)
becomes `drop index if exists {name} on {table};`. Casing stays lowercase to match the line it
replaces. All four callers (`TableDelta.cs:146, 149, 345, 353`) pick it up.

**Foreign key add.** `ForeignKey.WriteAddStatement` (`ForeignKey.cs:136-146`) is guarded in
place:

```
IF OBJECT_ID(N'dbo.fk_state', N'F') IS NULL
ALTER TABLE dbo.people
ADD CONSTRAINT fk_state FOREIGN KEY(state_id) REFERENCES dbo.states(id);
```

A constraint is a schema-scoped object, so the guard names `{parent.Identifier.Schema}.{Name}`.
Unlike the index, nothing compares `ForeignKey.ToDDL` text (`Equals` at `ForeignKey.cs:105-119`
compares fields), so the guard can live in `WriteAddStatement` itself and `ToDDL` (`:128-134`)
inherits it. That covers every emission site in one move: `Table.cs:274`, deferred keys at
`TableDelta.cs:38`, and the update and rollback paths at `TableDelta.cs:231, 238, 297, 304`.

**Table create.** The FK and index loops in `Table.WriteCreateStatement` stay outside the
`BEGIN ... END` block. Moving them inside would skip an index that a pre-existing table is
missing, and the `DropThenCreate` branch (`Table.cs:170-184`) has no block at all. The per-object
guards are what make the whole statement re-runnable.

### 6. What does not change

- `IndexDefinition.ToDDL` output and the `CanonicizeDdl` comparison path.
- `View`, `Function`, `Trigger`, `Synonym` emission.
- Schema creation and drop SQL.
- The dead PL/pgSQL `BeginScript` / `EndScript` constants (`SqlServerMigrator.cs:13-18`) and
  `WriteScript` still ignoring `IsTransactional`.

`WriteScript` itself did change, though only by a header. It now writes `SET QUOTED_IDENTIFIER ON;`
and a blank line before the inner step. sqlcmd is the one client that leaves that setting off, and
SQL Server will not create a filtered index, an index on a computed column or an indexed view while
it is off, so the very script this issue is about failed under a plain `sqlcmd -i` even with every
guard in place: the batch aborted at `idx_child_open_status`, the `CREATE TYPE` later in that same
batch never ran, the procedure in the next batch could not resolve its parameter type, and sqlcmd
exited 0 regardless. Documenting the `-I` flag was considered and rejected: a generated script
should not need flags to run. The header goes in the script wrapper, so the runtime executor, which
writes deltas straight out through `WriteUpdate`, never sees it, which is right because SqlClient
already defaults the setting on. No `GO` is needed after it, since `SET QUOTED_IDENTIFIER` applies
at parse time to the batch containing it and then persists for the session.

`TableType` and `Sequence` were on that list while it was being drafted, and both had to come off
it. Each emits a bare `CREATE` with no `OR ALTER` and no `IF NOT EXISTS` of its own, so a second
run of the rendered script raises "The type ... already exists" or "There is already an object
named ... in the database" and aborts everything after it. `TableType.WriteCreateStatement` now
leads with `IF TYPE_ID(N'schema.name') IS NULL` and `Sequence.WriteCreateStatement` with
`IF OBJECT_ID(N'schema.name', N'SO') IS NULL`. The `CREATE` lines themselves are unchanged.

## Trade-offs

- **`GO` in an executor that also runs hand-written SQL.** Only rendered-DDL executors split.
  `RunSqlAsync` and the partition managers keep sending text straight through, so a user
  statement that happens to contain a `GO` line behaves as before.
- **Ignoring `GO n`.** Faithful sqlcmd would repeat the batch. Repeating DDL is never a migration
  intent, and honouring it would turn a typo into n index creations. Documented, not honoured.
- **Guard text in the FK statement changes `ToDDL` output.** Accepted because nothing compares
  it; the alternative (a separate guarded method plus updating six call sites) buys nothing.
- **A second `IF` in front of every index makes scripts longer.** That is the point of the issue.

## Tests that touch the changed text

| Test | Today | After |
| --- | --- | --- |
| `src/Weasel.SqlServer.Tests/SchemaMigrationTests.cs:119, 135, 151` | NSubstitute mocks; assert which method was called, not text | unchanged |
| `src/Weasel.SqlServer.Tests/Tables/foreign_key_cycles.cs:120, 144` | `WriteAllUpdates` compared to concatenated `WriteCreateStatement` | unchanged, both sides gain the same guards |
| `foreign_key_cycles.cs:167-174` | `IndexOf` ordering of FK name vs `CREATE TABLE` | unchanged; the guard line carries the FK name and is still written after the table |
| `src/Weasel.SqlServer.Tests/Tables/IndexDefinitionTests.cs:33-92` | exact `ToDDL` text | unchanged, `ToDDL` is not guarded |
| `src/Weasel.SqlServer.Tests/Tables/ForeignKeyTests.cs:25-27, 81` | `ShouldContain` on `ToDDL` | unchanged; the guarded text still contains every asserted fragment |
| `src/Weasel.SqlServer.Tests/Tables/TableTests.cs:113-114, 136` | `lines.ShouldContain("CREATE TABLE dbo.people (")` | unchanged |
| `src/Weasel.SqlServer.Tests/Procedures/StoredProcedureTests.cs` | integration through `CreateAsync` / `ApplyChangesAsync` | passes once the executors split; gains the `CREATE OR ALTER` authored body cases |
| `src/DocSamples/SqlServerSamples.cs:165-169` | comments say "CREATE PROCEDURE" | comments updated; code compiles as is |
| `docs/core/procedures.md:64` | "via `WriteCreateOrAlterStatement`" | row updated to say both paths |

## Follow-ups (explicitly out of scope)

1. Remove the dead PL/pgSQL `BeginScript` / `EndScript` constants in `SqlServerMigrator.cs:13-18`
   and make `WriteScript` (`:91-94`) honour `Migrator.IsTransactional`, so `db-patch
   --transactional` stops silently doing nothing on SQL Server.
2. Convert `View`, `Function` and `Trigger` from drop-then-`sp_executesql` to `CREATE OR ALTER`.
3. A reflection-driven cross-provider guard test, in the style of
   `src/Weasel.Core.Tests/introspection_queries_terminate_themselves.cs`, that renders every
   `ISchemaObject` twice and executes the result, so the next unguarded object fails the build.

## Open questions

1. `GO n` repeat counts: this design accepts and ignores the count. If the maintainers want
   sqlcmd-faithful repetition instead, the splitter returns the batch n times and the test
   changes; nothing else moves.
2. The `Migrator.SplitIntoBatches` seam adds one public virtual to `Weasel.Core`. The
   alternative is to leave `SchemaMigration.RollbackAllAsync` unsplit and document that SQL Server
   rollbacks containing a procedure must go through `SqlServerMigrator`. The seam is smaller than
   the caveat.
