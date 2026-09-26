# Upgrading to 9.35

9.35.0 carries two independent fixes. A generated SQL Server migration script now runs as one file
under `sqlcmd` or SSMS, and runs a second time against the same database without failing
([#593](https://github.com/JasperFx/weasel/issues/593)). And `BatchedQuery` in
`Weasel.EntityFrameworkCore` now returns exactly what EF Core returns
([#621](https://github.com/JasperFx/weasel/issues/621)). No other provider is affected.

::: tip Coming from 9.34?
Two things to know.

**SQL Server.** Rendered stored procedure DDL now carries `GO` lines, so a consumer that executes
that text through its own `SqlCommand` has to split it first. Everything else is a text change
inside DDL Weasel itself executes.

**EF Core.** A batched query used to return incomplete entities, silently. If you batch queries —
directly, or through Wolverine's query plans, which batch themselves as soon as a handler has two
of them — read [the section below](#ef-core-batched-queries-return-what-ef-core-returns). Saving an
entity that came back from a batch could write `NULL` over data.
:::

## ⚠️ Stored procedure DDL is emitted in its own batch

[#593](https://github.com/JasperFx/weasel/issues/593).

T-SQL requires a procedure definition to be the only statement in its batch. A rendered migration
concatenates every object's DDL into one script, so a `CREATE INDEX` running straight into a
`CREATE OR ALTER PROCEDURE` is a script SQL Server rejects. It never reproduced per object, because
the runtime apply path sends one command per delta and a procedure always landed in a batch of its
own by accident.

`StoredProcedure.WriteCreateStatement` and `StoredProcedure.WriteCreateOrAlterStatement` now emit
the same text on both paths, wrapped in batch separators:

```sql
GO
CREATE OR ALTER PROCEDURE procs.uspDeleteIncomingEnvelopes
    @IDLIST procs.EnvelopeIdList READONLY
AS
    DELETE FROM procs.incoming WHERE id IN (SELECT ID FROM @IDLIST);
GO
```

The body's own `CREATE PROCEDURE`, `CREATE PROC` or `CREATE OR ALTER PROC` preamble is normalised to
`CREATE OR ALTER PROCEDURE` in place, whatever its casing, so the create path is idempotent too. Only
the first such token is touched, which leaves the same words inside a later string literal alone.

**`GO` is not T-SQL.** `SqlClient` answers `Incorrect syntax near 'GO'` if the text reaches it whole.
Weasel's own executors all split first: `SchemaObjectsExtensions.CreateAsync`,
`SchemaObjectsExtensions.Drop`, `Migrator.ApplyAllAsync` through `SqlServerMigrator`, and
`SchemaMigration.RollbackAllAsync`. Nothing you drive through those needs a change.

**If you execute rendered DDL text yourself**, split it before you send it:

```csharp
var sql = new StringWriter();
procedure.WriteCreateStatement(migrator, sql);

foreach (var batch in SqlServerBatchSplitter.Split(sql.ToString()))
{
    await using var command = connection.CreateCommand();
    command.CommandText = batch;
    await command.ExecuteNonQueryAsync(token);
}
```

`SqlServerBatchSplitter.Split` returns the non-empty batches in order, each trimmed. Switching that
call site to `procedure.CreateAsync(connection)` works just as well and is the smaller change. The
splitter's semantics are sqlcmd's, deliberately including its bluntness: a line whose whole content
is `GO` ends the batch wherever it appears, case insensitively, with an optional repeat count that is
accepted and ignored. String literals and comments are not parsed, and a trailing comment on the same
line as `GO` means the line is not a separator at all.

## Rendered create DDL carries existence guards

Running a generated script twice used to fail on the first unguarded object, and one failure aborts
every statement after it in that batch. Each of these now guards itself:

| Emitted by | Guard line |
| --- | --- |
| `ForeignKey.WriteAddStatement`, and so `ForeignKey.ToDDL` | `IF OBJECT_ID(N'dbo.fk_state', N'F') IS NULL` |
| `IndexDefinition.WriteCreateStatement` | `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_1' AND object_id = OBJECT_ID(N'dbo.people'))` |
| `StringWriterExtensions.WriteDropIndex` | `drop index if exists` |
| `TableType.WriteCreateStatement` | `IF TYPE_ID(N'dbo.ChildIdList') IS NULL` |
| `Sequence.WriteCreateStatement` | `IF OBJECT_ID(N'dbo.seq_people', N'SO') IS NULL` |

`CREATE TABLE` and `CREATE SCHEMA` were already guarded and are unchanged.

`IndexDefinition.ToDDL` is **not** guarded, because delta comparison canonicalises its text. The
guard lives at the emission sites, reached through the new
`IndexDefinition.WriteCreateStatement(Table, TextWriter)`. So any assertion you hold against `ToDDL`
still passes; an assertion against a rendered table or migration script gains the guard lines.

`DROP INDEX IF EXISTS` and `CREATE OR ALTER` mean **SQL Server 2016 SP1 or later** is now required.
That was already the effective floor, since `DROP CONSTRAINT IF EXISTS` has been emitted for some
time.

## Generated script files set `QUOTED_IDENTIFIER` on

Files written by `Migrator.WriteMigrationFileAsync`, `Migrator.WriteTemplatedFile` and
`IDatabase.ToDatabaseScript` now begin with:

```sql
SET QUOTED_IDENTIFIER ON;
```

The `-drop` companion file that `WriteMigrationFileAsync` writes alongside gets the same header.

sqlcmd is the one client that leaves the setting off, and SQL Server refuses to create a filtered
index, an index on a computed column or an indexed view while it is off. So the very script this
issue is about still failed under a plain `sqlcmd -i` with every guard in place, quietly: the batch
aborted at the filtered index, the `CREATE TYPE` later in that same batch never ran, the procedure in
the next batch could not resolve its parameter type, and sqlcmd exited 0 regardless. Documenting the
`-I` flag was considered and rejected, because a generated script should not need flags to run.

The header is written by the script wrapper, so the runtime executor, which writes deltas straight
out through `WriteUpdate`, never sees it. That is right: `SqlClient` already defaults
`QUOTED_IDENTIFIER` on. No `GO` is needed after the line either, because `SET QUOTED_IDENTIFIER`
takes effect at parse time for the batch containing it and then persists for the session.

## Schema fingerprinting re-stamps once

If you run with `UseSchemaFingerprinting`, the fingerprint is a hash of the rendered create text, and
that text changed. The first apply after upgrading will therefore see a stale stamp, do one full
apply, and record the new fingerprint. Subsequent runs short-circuit as before. This is self-healing
and needs no action.

## EF Core bridge: a persisted snapshot holding a raw object needs regenerating

`Weasel.EntityFrameworkCore` captures anything EF cannot model as raw SQL in the snapshot it
persists: SQL Server stored procedures and table types, plus any table you forced raw. The captured
create text changed in this release, so the next `db-ef-migration add` against a snapshot written by
an older version throws:

```
System.NotSupportedException: The raw-SQL schema object 'procs.uspDeleteIncomingEnvelopes' changed
since the last snapshot. The snapshot diff cannot infer a safe transformation for raw objects ...
```

The diff is refusing to guess, which is correct in general and noise here, since the object itself did
not change. Regenerate the migration against the live database instead of the snapshot:

```bash
dotnet run -- db-ef-migration add MyMigration --against-database
```

That path introspects the real schema rather than diffing two renders, writes a correct migration,
and rewrites the snapshot with the new text, after which plain `db-ef-migration add` works again. A
SQL Server snapshot with no raw objects in it is unaffected.

## EF Core batched queries return what EF Core returns

[#621](https://github.com/JasperFx/weasel/issues/621), reported and fixed by
[@ayuksekkaya](https://github.com/ayuksekkaya).

`BatchedQuery.Query<T>()` and `QuerySingle<T>()` used to build entities by reflection over the
entity type's scalar properties, which is everything EF Core's own query pipeline is not. Anything
that is not flat came back incomplete, and nothing reported an error:

- owned types, owned JSON columns, complex properties and complex collections were left `null` or
  empty
- `Include`d navigations were empty, and a list query with a collection `Include` returned one
  parent per child row
- results were never tracked, even for a tracking query, and an entity the context already tracked
  came back as a second instance

**Saving one of those entities wrote the empty members to the database.** A required complex
property failed the save instead.

EF Core now runs every batched query and materializes every result, so a batched query returns
exactly what the same query returns on its own: tracking, identity resolution, owned, complex and
JSON members, `Include`s and projections all behave as usual. The batch still goes to the database
in one round trip. `Query<T>` and `QuerySingle<T>` also lost their `where T : class, new()`
constraint, so a projection into a DTO can be batched.

### Registering the interceptor

One round trip requires the new `BatchedQueryInterceptor` on the `DbContext`:

```csharp
services.AddDbContext<MyDbContext>(opts =>
{
    opts.UseNpgsql(connectionString);
    opts.UseWeaselBatchedQueries();
});
```

Without it — and in a handful of cases where a batch cannot faithfully stand in for separate
execution: another command interceptor is registered, the execution strategy retries on failure, or
a query is a split query — every queued query runs on its own round trip instead, with the same
results. So the correctness of this release does not depend on registering anything; only the round
trip count does. A warning is logged once per `DbContext` type explaining which case applied.

::: warning Wolverine users
Wolverine batches query plans automatically as soon as a handler has two of them against the same
`DbContext`, which means this defect could be introduced into a working handler by adding an
unrelated second plan. Wolverine registers the interceptor for you in
`AddDbContextWithWolverineIntegration` from the release that takes this version of Weasel; until
then your plans are correct and take one round trip each.
:::

## New public API

- `Migrator.SplitIntoBatches(string)`, `public virtual`, returning the whole string as a single batch.
  This is the seam `SchemaMigration.RollbackAllAsync` uses so a rollback containing a procedure
  executes correctly. Every provider but SQL Server keeps the default.
- `SqlServerBatchSplitter.Split(string)`, `public static`, the sqlcmd-semantics splitter described
  above. `SqlServerMigrator` overrides `SplitIntoBatches` to call it.
- `IndexDefinition.WriteCreateStatement(Table, TextWriter)`, the guarded emission method that the
  table and delta paths now use in place of writing `ToDDL` directly.
- `BatchQueryExtensions.UseWeaselBatchedQueries(DbContextOptionsBuilder)` and its generic sibling,
  which register `BatchedQueryInterceptor` on a `DbContext`.
- `BatchedQueryInterceptor`, the `DbCommandInterceptor` that hands EF Core a queued query's result
  set out of the batch instead of executing the command.
