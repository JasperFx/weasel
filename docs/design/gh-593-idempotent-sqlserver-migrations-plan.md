# gh-593 implementation plan

Design: `docs/design/gh-593-idempotent-sqlserver-migrations.md`. Read it first; this file only
orders the work.

## Context

Ten small tasks, each test-first. Tasks 1 to 4 make the procedure path valid in a concatenated
script and re-runnable; tasks 5 to 7 make index and foreign key DDL re-runnable; task 8 is the
one regression test that covers the issue as reported; tasks 9 and 10 are docs and a final sweep.

Conventions: `.editorconfig` (4 spaces, `_camelCase` private fields, nullable enabled), no
trailing whitespace, no em dashes. Integration tests need `weasel_sqlserver_testing_database` set
and a reachable SQL Server (`docker compose up` at the repo root).

Base test command, used with a `--filter` per task:

```
dotnet test D:\code\github\jasperfx\weasel\src\Weasel.SqlServer.Tests\Weasel.SqlServer.Tests.csproj --framework net9.0
```

## Task 1: the batch splitter

Files:

- new `src/Weasel.SqlServer/SqlServerBatchSplitter.cs`
- new `src/Weasel.SqlServer.Tests/SqlServerBatchSplitterTests.cs`

Behaviour: `SqlServerBatchSplitter.Split(string sql)` returns the non-empty batches of `sql`
separated by lines matching `^\s*GO(?:\s+(\d+))?\s*;?\s*$` (multiline, case insensitive). The
count is accepted and ignored. Batches keep their internal line endings; leading and trailing
blank lines of a batch are trimmed.

Test first (plain xUnit, no database):

- two statements separated by `GO` give two batches
- `go` lowercase and `GO;` and `  GO  ` (indented, trailing spaces) are all separators
- `GO 3` is a separator and the batch appears once
- a leading `GO` and a trailing `GO` produce no empty batches
- consecutive `GO` lines produce no empty batches
- CRLF input splits the same as LF input
- a line reading `GO` inside a string literal is still a separator (sqlcmd parity, documented)
- `GOTO label` and `select 'GO' as x` are not separators
- text with no `GO` comes back as one batch

Verify:

```
dotnet test D:\code\github\jasperfx\weasel\src\Weasel.SqlServer.Tests\Weasel.SqlServer.Tests.csproj --framework net9.0 --filter "FullyQualifiedName~SqlServerBatchSplitterTests"
```

## Task 2: route every rendered-DDL executor through the splitter

Files:

- `src/Weasel.Core/Migrator.cs`: add `public virtual IReadOnlyList<string> SplitIntoBatches(string sql) => [sql];`
- `src/Weasel.Core/SchemaMigration.cs:533-541` (`RollbackAllAsync`): loop over
  `rules.SplitIntoBatches(writer.ToString())`, one `CreateCommand` per batch
- `src/Weasel.SqlServer/SqlServerMigrator.cs`: override `SplitIntoBatches` to call the splitter;
  change `executeCommand` (`:224-243`) to iterate batches, `logger.SchemaChange` per batch, and on
  a non-default logger call `logger.OnFailure` with the failing batch's command and return without
  running the rest of that text
- `src/Weasel.SqlServer/SchemaObjectsExtensions.cs:29-43` (`Drop`, `CreateAsync`): iterate batches
- `src/Weasel.SqlServer.Tests/IntegrationContext.cs:34-59`: `CreateSchemaObjectInDatabase` calls
  `schemaObject.CreateAsync(theConnection)` and `DropSchemaObjectInDatabase` calls
  `schemaObject.Drop(theConnection)`, keeping the existing "DDL Execution Failure" wrapping

Test first: a `Weasel.SqlServer.Tests` integration test, new file
`src/Weasel.SqlServer.Tests/batch_separated_ddl_executes.cs` (class `IntegrationContext`
subclass, schema `batches`), with a fake `ISchemaObject` whose `WriteCreateStatement` writes
`create table batches.a (id int);` then `GO` then `create table batches.b (id int);`. Assert
`CreateAsync` succeeds and both tables exist (`ExistingTables`, `SchemaObjectsExtensions.cs:229`).
A second test applies the same object through `new SqlServerMigrator().ApplyAllAsync` with a
`SchemaMigration` built from a delta reporting `Create` and asserts the same. Both fail before
this task with a `SqlException` on `GO`.

Verify:

```
dotnet test D:\code\github\jasperfx\weasel\src\Weasel.SqlServer.Tests\Weasel.SqlServer.Tests.csproj --framework net9.0 --filter "FullyQualifiedName~batch_separated_ddl_executes"
dotnet test D:\code\github\jasperfx\weasel\src\Weasel.Core.Tests\Weasel.Core.Tests.csproj --framework net9.0
```

## Task 3: procedures emit GO and CREATE OR ALTER on both paths

Files:

- `src/Weasel.SqlServer/Procedures/StoredProcedure.cs`: add
  `internal static string NormalizeCreateStatement(string body)`, override
  `WriteCreateStatement`, rewrite `WriteCreateOrAlterStatement` (`:56-63`) so both write
  `GO`, the normalised body, `GO`, each on its own line, and both return early when `IsRemoved`
- `src/Weasel.SqlServer/Canonicalization.cs`: add the procedure preamble regex next to
  `CreatePreamble` (`:13-14`); task 4 uses it for comparison, this task uses it for emission

Behaviour: the leading `CREATE [OR ALTER] PROC[EDURE]` token (case insensitive, after any leading
whitespace or comments) becomes `CREATE OR ALTER PROCEDURE`; a body already in that form is
unchanged; later occurrences (inside literals) are untouched.

Test first, new file `src/Weasel.SqlServer.Tests/Procedures/stored_procedure_ddl.cs` (no
database):

- `WriteCreateStatement` output for a `CREATE PROCEDURE` body is exactly
  `GO\r\nCREATE OR ALTER PROCEDURE ...\r\nGO\r\n` (use `Environment.NewLine`)
- `WriteCreateOrAlterStatement` output is identical to `WriteCreateStatement` output
- `CREATE PROC`, `create procedure`, `Create Or Alter Proc`, and a body with a leading
  `-- comment` line each normalise to `CREATE OR ALTER PROCEDURE`
- a body already `CREATE OR ALTER PROCEDURE` is not double-rewritten
- a body whose text contains `'CREATE PROCEDURE'` inside a string literal after the preamble keeps
  that literal intact
- `IsRemoved` writes nothing
- `BodyText()` and `CanonicizeSql()` contain no `GO`

Then confirm the existing integration file still passes; it exercises `CreateAsync` and
`ApplyChangesAsync`, which now go through task 2's splitter:

```
dotnet test D:\code\github\jasperfx\weasel\src\Weasel.SqlServer.Tests\Weasel.SqlServer.Tests.csproj --framework net9.0 --filter "FullyQualifiedName~stored_procedure_ddl|FullyQualifiedName~StoredProcedureTests"
```

## Task 4: procedure delta ignores the OR ALTER preamble

Files:

- `src/Weasel.SqlServer/Procedures/StoredProcedureDelta.cs:23-25`: run both `CanonicizeSql()`
  results through the preamble normaliser (bare `CREATE PROCEDURE` form) before comparing

Behaviour: a body authored as `CREATE OR ALTER PROCEDURE` compares equal to the catalog's
`CREATE   PROCEDURE` rendering (verified on the local Docker SQL Server, see the design), so
the delta is `None` after apply instead of `Update` forever.

Test first, in `src/Weasel.SqlServer.Tests/Procedures/StoredProcedureTests.cs`:

- `authored_as_create_or_alter_round_trips`: construct with a `CREATE OR ALTER PROCEDURE` body,
  `CreateAsync`, `FindDeltaAsync` is `None`
- `authored_as_create_proc_round_trips`: same with `CREATE PROC`
- `apply_update_delta` and `fetch_delta_with_different_body` keep passing (a real body change is
  still `Update`)

Verify:

```
dotnet test D:\code\github\jasperfx\weasel\src\Weasel.SqlServer.Tests\Weasel.SqlServer.Tests.csproj --framework net9.0 --filter "FullyQualifiedName~StoredProcedureTests"
```

## Task 5: guarded CREATE INDEX at every emission site

Files:

- `src/Weasel.SqlServer/Tables/IndexDefinition.cs`: add
  `public void WriteCreateStatement(Table parent, TextWriter writer)`; `ToDDL` unchanged
- `src/Weasel.SqlServer/Tables/Table.cs:281`
- `src/Weasel.SqlServer/Tables/TableDelta.cs:186, 189, 348, 354`

Behaviour: the four sites write
`IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'<name>' AND object_id = OBJECT_ID(N'<schema.table>'))`
on one line and `    ` plus `ToDDL(parent)` on the next. Literals go through
`SchemaUtils.EscapeLiteral`; the table spelling is `parent.Identifier.QualifiedName` as at
`Table.cs:187`.

Test first:

- `src/Weasel.SqlServer.Tests/Tables/IndexDefinitionTests.cs`: `write_create_statement_is_guarded`
  asserting the exact two-line text for `idx_1` on `dbo.people`, and a case with a name containing
  an apostrophe to prove escaping
- `src/Weasel.SqlServer.Tests/Tables/TableTests.cs`: `create_statement_guards_each_index`,
  render a table with two indexes and assert each index's guard line appears once
- `src/Weasel.SqlServer.Tests/Tables/detecting_table_deltas.cs`: an update delta with a missing
  index writes the guard line (`ShouldContain("IF NOT EXISTS (SELECT 1 FROM sys.indexes")`)

Verify, then the full table suite to confirm nothing that compares `ToDDL` moved:

```
dotnet test D:\code\github\jasperfx\weasel\src\Weasel.SqlServer.Tests\Weasel.SqlServer.Tests.csproj --framework net9.0 --filter "FullyQualifiedName~Weasel.SqlServer.Tests.Tables"
```

## Task 6: DROP INDEX IF EXISTS

Files:

- `src/Weasel.SqlServer/Tables/StringWriterExtensions.cs:23-26`

Behaviour: `drop index if exists {QuoteName(index.Name)} on {table.Identifier};`. Callers at
`TableDelta.cs:146, 149, 345, 353` need no change.

Test first:

- `src/Weasel.SqlServer.Tests/Tables/detecting_table_deltas.cs`: an update delta with an extra
  index writes `drop index if exists`
- `src/Weasel.SqlServer.Tests/Tables/rolling_back_table_deltas.cs`: rollback of a missing index
  writes `drop index if exists`

Verify:

```
dotnet test D:\code\github\jasperfx\weasel\src\Weasel.SqlServer.Tests\Weasel.SqlServer.Tests.csproj --framework net9.0 --filter "FullyQualifiedName~detecting_table_deltas|FullyQualifiedName~rolling_back_table_deltas"
```

## Task 7: guarded foreign key add

Files:

- `src/Weasel.SqlServer/Tables/ForeignKey.cs:136-146` (`WriteAddStatement`)

Behaviour: the first line becomes
`IF OBJECT_ID(N'<schema>.<fk name>', N'F') IS NULL`, followed by the existing `ALTER TABLE ...
ADD CONSTRAINT ...` lines unchanged. Schema is `parent.Identifier.Schema`; both literals escaped.
`ToDDL` and all six emission sites (`Table.cs:274`, `TableDelta.cs:38, 231, 238, 297, 304`)
inherit the guard.

Test first:

- `src/Weasel.SqlServer.Tests/Tables/ForeignKeyTests.cs`: `write_fk_ddl_is_guarded` asserting the
  exact first line for `fk_state` on `dbo.people`; the existing `ShouldContain` tests stay
- `src/Weasel.SqlServer.Tests/Tables/foreign_key_cycles.cs`: the three text tests at `:106-175`
  must pass unmodified; run them as the regression check for deferred keys

Verify:

```
dotnet test D:\code\github\jasperfx\weasel\src\Weasel.SqlServer.Tests\Weasel.SqlServer.Tests.csproj --framework net9.0 --filter "FullyQualifiedName~ForeignKeyTests|FullyQualifiedName~foreign_key_cycles"
```

## Task 8: the issue-shaped regression test

Files:

- new `src/Weasel.SqlServer.Tests/migration_scripts_are_idempotent.cs`

Behaviour under test, end to end:

1. Build a `DatabaseWithTables` (`src/Weasel.SqlServer/DatabaseWithTables.cs`) in schema
   `gh593` with a parent table, a child table carrying a foreign key to the parent and two
   indexes (one unique, one filtered), a `TableType`, and a `StoredProcedure` whose body is
   authored as `CREATE OR ALTER PROCEDURE` and takes the table type (the shape in
   `StoredProcedureTests.cs:61-67`).
2. `CreateMigrationAsync`, then render with `SchemaMigration.WriteAllUpdates` after
   `WriteSchemaCreationSql`, the same order as `Migrator.WriteMigrationFileAsync`
   (`src/Weasel.Core/Migrator.cs:270-278`). Also call `WriteMigrationFileAsync` to a temp file
   and assert the file text equals the rendered text.
3. Assert the text contains a `GO` line immediately before and after the procedure definition
   and does not contain a bare `CREATE PROCEDURE`.
4. Execute the whole script through `SqlServerBatchSplitter.Split`, one `SqlCommand` per batch.
5. Execute it a second time the same way: no exception.
6. `AssertDatabaseMatchesConfigurationAsync` passes, and `CreateMigrationAsync` reports
   `SchemaPatchDifference.None`.
7. Negative control, guarded by the splitter test only: executing the script as one command
   without splitting throws a `SqlException` mentioning `GO`, which documents why the splitter
   exists.

Verify:

```
dotnet test D:\code\github\jasperfx\weasel\src\Weasel.SqlServer.Tests\Weasel.SqlServer.Tests.csproj --framework net9.0 --filter "FullyQualifiedName~migration_scripts_are_idempotent"
```

## Task 9: documentation

Files:

- `docs/sqlserver/index.md`: new section "Batch separators and re-runnable scripts" after
  "Schema Management": generated scripts contain `GO` lines around procedures; `sqlcmd` and SSMS
  handle them; Weasel's own executors split on `GO` with sqlcmd semantics (a line that is only
  `GO`, case insensitive, optional count ignored, literal context not parsed); index, foreign key
  and table creation are guarded so a script can be run twice; `DROP INDEX IF EXISTS` and
  `CREATE OR ALTER` mean SQL Server 2016 SP1 or later.
- `docs/sqlserver/procedures.md`: "Generating DDL" now says both `WriteCreateStatement` and
  `WriteCreateOrAlterStatement` emit `CREATE OR ALTER PROCEDURE` between `GO` lines, that the
  leading `CREATE PROC` token is normalised, and that a body authored as `CREATE OR ALTER` is
  compared correctly against the catalog.
- `docs/sqlserver/tables.md`: one paragraph under "Indexes" and one under "Foreign Keys"
  showing the guard text.
- `docs/core/procedures.md:64`: SQL Server row reads "`CREATE OR ALTER PROCEDURE` on both create
  and update, wrapped in `GO`".
- `src/DocSamples/SqlServerSamples.cs:165-169`: comments updated to match; no code change.
- `docs/.vitepress/config.ts`: no change, no new page.

Verify: `npm run docs:build` from `D:\code\github\jasperfx\weasel` if the docs toolchain is
installed locally; otherwise a read-through of the three pages.

## Task 10: full sweep

Verify everything together, including the EF bridge's SQL Server tests, which apply through
`Migrator.ApplyAllAsync` and emit `WriteAllUpdates` text into migration files:

```
dotnet build D:\code\github\jasperfx\weasel\Weasel.slnx -c Release
dotnet test D:\code\github\jasperfx\weasel\src\Weasel.SqlServer.Tests\Weasel.SqlServer.Tests.csproj --framework net9.0
dotnet test D:\code\github\jasperfx\weasel\src\Weasel.Core.Tests\Weasel.Core.Tests.csproj --framework net9.0
dotnet test D:\code\github\jasperfx\weasel\src\Weasel.EntityFrameworkCore.Tests\Weasel.EntityFrameworkCore.Tests.csproj --framework net9.0 --filter "FullyQualifiedName~SqlServer"
```

Then a manual check that matches the issue: run `db-patch` from any SQL Server sample against a
database that already has the schema applied, open the file, and run it with `sqlcmd -i` twice.

## Verification summary

- Unit: splitter cases, exact procedure text, exact index guard text, exact FK guard text,
  `drop index if exists`.
- Integration: `StoredProcedureTests` (both authored forms round-trip to `None`),
  `batch_separated_ddl_executes`, `migration_scripts_are_idempotent` (script executes twice,
  database matches configuration).
- Regression: every existing `Weasel.SqlServer.Tests` test unchanged and green; `Weasel.Core.Tests`
  green because of the new `Migrator.SplitIntoBatches` virtual; EF bridge SQL Server tests green.
