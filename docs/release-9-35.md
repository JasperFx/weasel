# Upgrading to 9.35

9.35.0 carries two independent fixes. A generated SQL Server migration script now runs as one file
under `sqlcmd` or SSMS, and runs a second time against the same database without failing
([#593](https://github.com/JasperFx/weasel/issues/593)). And `BatchedQuery` in
`Weasel.EntityFrameworkCore` now returns exactly what EF Core returns
([#621](https://github.com/JasperFx/weasel/issues/621)). No other provider is affected.

**[9.35.2](#_9-35-2)** is one PostgreSQL fix: an exhausted `SchemaUtils.DropSchema` retry reported
success instead of throwing.

**[9.35.1](#_9-35-1) is an EF Core release, and one of its fixes is urgent**: a table-split
`ComplexProperty` was not mapped at all, and a migration against a table EF Core created **dropped**
its columns — silently, on lowercase column names. If you use `ComplexProperty` with a Weasel-managed
EF migration, read [9.35.1](#_9-35-1) before upgrading. It also changes what an existing database does
on its next migration, which is unusual for a patch and is called out there.

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

### A procedure body with a `GO` line is refused

Rendering a procedure whose body contains a line whose entire content is `GO` now throws:

```
The body of stored procedure reporting.rebuild contains a line whose entire content is GO.
```

This is the one hazard the bracketing carries, and without the refusal it is silent. Neither sqlcmd
nor `SqlServerBatchSplitter` parses string literals, so such a line ends the batch wherever it
appears — including in the middle of a definition, which submits the fragment before it as a complete
procedure and the fragment after it as a statement of its own. The result is a syntax error pointing
at the tail of somebody's dynamic SQL, or a procedure that compiles and is not the one that was
written.

A body is T-SQL somebody wrote, and T-SQL that builds scripts is normal, so this is reachable rather
than theoretical. The refusal uses the splitter's own definition of a separator, so `GOTO`, the word
inside a literal, and `GO` with anything else on the line are all still fine.

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
execution: the database provider doesn't support `DbBatch` (SQLite and Oracle don't), another command
interceptor is registered, the execution strategy retries on failure, or a query is a split query — every queued query runs on its own round trip instead, with the same
results. So the correctness of this release does not depend on registering anything; only the round
trip count does. A warning is logged once per `DbContext` type explaining which case applied.

EF Core prepares each queued query once, when it is queued, and that prepared query supplies both the
batch's SQL and the results, so captured values are read once and at queue time, as in 9.34.
`BatchedQuery` takes the prepared query's command from `IRelationalQueryingEnumerable`, an EF Core
internal interface that has not changed since EF Core 5.0. If a version of EF Core doesn't provide
it, every queued query runs on its own round trip, as above.

A batch only accepts queries of the `DbContext` it was created for; queuing another context's query
throws. EF Core enumerates a collection used in `Contains()` each time it builds the query's command,
which a batch does more than once, so pass a list or array rather than a sequence that can only be
enumerated once.

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

## 9.35.1

An EF Core release: three issues, filed together and fixed together, all of which met in one place.
A table-split `ComplexProperty` produced a migration that **dropped** the columns EF Core had created
for it, and on lowercase column names that drop executed and destroyed the data
([#628](https://github.com/JasperFx/weasel/issues/628)).

::: danger Read this before upgrading if you use `ComplexProperty` with a Weasel-managed EF migration
Check those tables on your **current** version first. This is a bug that has already run.

A table-split `ComplexProperty` — one *without* `ToJson()` — was not mapped at all, on 9.35.0 and
every earlier version carrying the EF bridge. So:

- on a fresh database, the table was created without those columns and the first insert failed with
  `42703`
- against a table EF Core itself created, a `CreateOrUpdate` migration emitted
  `drop column total_amount`. With **PascalCase** column names that failed with `42703` and aborted
  the migration, which is how this was found. With **lowercase** column names — what
  `UseSnakeCaseNamingConvention()` or an explicit `HasColumnName` produces — **it succeeded, and the
  data is gone.**

Every Weasel-managed EF migration path runs through this mapper, including Wolverine's
`UseEntityFrameworkCoreWolverineManagedMigrations()` and its tenanted `DbContext` builders.
:::

::: warning A behaviour change, in a patch release
A migration for an EF-derived table **no longer drops columns, indexes or foreign keys that the EF
model does not declare.** That is deliberate and it is the fix for
[#629](https://github.com/JasperFx/weasel/issues/629), but it is not the kind of change a patch bump
usually carries, so it is called out here rather than left to be discovered.

If you relied on a removed EF property taking its column with it, set
`EfSchemaMappingCustomization.AllowDrops = true` — see [below](#restoring-the-old-behaviour). Tables
you define in Weasel directly are completely unaffected.
:::

Two things had to be true for the data loss to happen: the mapper did not translate complex
properties (#628), and `CreateOrUpdate` read a column the mapper could not express as a column the
developer had removed (#629). Both are fixed.
[#627](https://github.com/JasperFx/weasel/issues/627) is the third, and it is what made the failure
visible at all — the drop statement named the wrong identifier.

### ⚠️ Table-split complex properties are mapped

[#628](https://github.com/JasperFx/weasel/issues/628).

`MapToTable` reached a `ComplexProperty` through neither of its two walks. Complex properties are not
navigations, so the owned-type walk missed them, and they carry no `IEntityType` of their own, so
`GetProperties()` on the entity type did not reach their members either. The only code that looked at
`GetComplexProperties()` was the `ToJson()` container mapping from
[#291](https://github.com/JasperFx/weasel/issues/291), which skipped everything else.

So this model produced a table with `Id` and `Customer` and nothing else:

```csharp
modelBuilder.Entity<Order>(o =>
{
    o.ToTable("orders");
    o.ComplexProperty(x => x.Total);          // table-split: NOT mapped before 9.35.1
    o.OwnsOne(x => x.Address);                // table-split owned type: always mapped
});
```

Every member of a non-JSON complex type now becomes a column of the owner's table, exactly as a
table-split `OwnsOne`'s members do, with the same store type, nullability and default handling.
Nested complex properties are walked transitively, so
`ComplexProperty(x => x.Origin, c => c.ComplexProperty(o => o.Coordinates))` contributes
`Origin_Coordinates_Latitude`. An explicit `HasColumnName` on a complex member is honored.

A complex **collection** is unchanged: EF Core only maps one to JSON, which was already handled.

#### What this means for an existing database

- **A table EF Core created** (`dotnet ef database update`, `CreateTablesAsync`) is now correct:
  Weasel sees the complex columns, reports no delta, and stops wanting to drop them.
- **A table Weasel created** is missing those columns. The next migration adds them — as nullable
  columns, or as `NOT NULL` if the complex property is required, which cannot be applied to a table
  that already has rows. Such a delta is `Invalid`, which `CreateOrUpdate` refuses rather than
  applying; add the columns by hand, or run one `AutoCreate.All` against a database you are willing
  to rebuild.
- **Rows you inserted through EF Core against a table that was missing the columns** do not exist:
  the insert failed at the time. Nothing needs recovering.

#### A `ToJson()` container column is no longer always `jsonb`

Fixed in the same code. When EF Core's model does not name the container column's store type, the
fallback was the literal `"jsonb"` — on every provider. A `ToJson()` mapping therefore emitted a
`jsonb` column on SQL Server, which is not a type it has.

The fallback is now the provider's own JSON store type, matching each provider's `GetDatabaseType`
fallback, through the new `Migrator.DefaultJsonColumnType`:

| Provider | Type |
| --- | --- |
| PostgreSQL | `jsonb` |
| SQL Server | `nvarchar(max)` |
| MySQL | `TEXT` |
| SQLite | `TEXT` |
| Oracle | `CLOB` |

Only reached when the model leaves the type unspecified; a `ToJson("shipping", "jsonb")` or a
`HasColumnType` is unaffected.

### EF-derived tables are add-only by default

[#629](https://github.com/JasperFx/weasel/issues/629).

`AutoCreate.CreateOrUpdate` drops the columns, indexes and foreign keys the model no longer declares.
For a table you define in code, that is the point: the model is the whole truth about the schema.

For a table **translated** from an EF Core model it is not. The mapper reads EF Core's relational
model, and there are shapes it does not translate — TPC, entity splitting, temporal table period
columns, Npgsql enums and extensions, sequence min/max/cycle. A column EF Core knows about and the
mapper cannot express is not *removed from the model*; it is *not understood*. Reading it as a
removal made every gap in the translation a data-loss branch, which is exactly how #628 came to
execute a `DROP COLUMN`.

So every table from `MapToTable()` now sets the new `ITable.AddOnlyMigrations`:

- a column, index or foreign key the model does not declare is **left in place**
- everything additive still applies — new columns, new indexes, new foreign keys
- everything in-place still applies — a changed index is recreated, a widened type is altered
- only the *removal of something undeclared* is withheld

A migration whose only difference is a withheld drop reports `SchemaPatchDifference.None`, so it is
not an update that writes nothing.

`AddOnlyMigrations` is `Weasel.Core` API that all five providers implement, but nothing except the EF
mapper sets it, so a table you define yourself behaves exactly as it did in 9.35.0.

#### The withheld drops are reported

Each table delta carries them, and the apply path logs them once per table before running:

```
Not dropping column period_start from ef_add_only.AddOnlyOrders: the table is marked
AddOnlyMigrations, so a migration never removes what the model does not declare. ...
```

That goes through a new **defaulted** member on `IMigrationLogger`, so an existing implementation
gains it without being changed; override `WithheldDrop(string)` to route it at Warning into a real
logger. A schema whose *only* difference is a withheld drop has nothing to migrate and is
deliberately **not** warned about — that line would otherwise appear on every application start for
as long as the column exists. Read `ISchemaObjectDeltaWithWithheldDrops.WithheldDrops` off the delta
for the quiet case.

#### Restoring the old behaviour

When the EF model really is the whole truth about these tables:

```csharp
var customization = new EfSchemaMappingCustomization { AllowDrops = true };

await using var migration = await services.CreateMigrationAsync(context, customization, token);
await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, token);
```

The same customization works on `CreateDatabase`. For one table rather than all of them, clear the
flag from `CustomizeTable`, which runs after the mapping and so has the last word:

```csharp
CustomizeTable = (entityType, table) =>
{
    if (entityType.ClrType == typeof(StagingRow)) table.AddOnlyMigrations = false;
}
```

::: warning
`AddOnlyMigrations` is about *removal*, not about data. It does not make `AutoCreate.All` safe: a
change that can only be applied by dropping and recreating the table still drops it.
`Migrator.RefuseDestructiveChanges` ([#600](https://github.com/JasperFx/weasel/issues/600)) is the
control for that.
:::

### PostgreSQL: a drop-column statement names the identifier the catalog holds

[#627](https://github.com/JasperFx/weasel/issues/627).

A delta drops an extra column by rendering it from the **actual** column — the one read back from
`information_schema` — and that column was built with its name folded to lowercase. So against a
table whose columns are quoted and mixed-case, which is every EF-derived table (`MapToTable` sets
`PreserveIdentifierCase`), the delta emitted

```sql
alter table cp_ef.orders drop column total_amount;
```

for a column PostgreSQL calls `"Total_Amount"`, and the migration died with
`42703: column "total_amount" of relation "orders" does not exist`.

An introspected column now carries the spelling the catalog reported. Name comparison was already
case-insensitive everywhere it matters — `TableColumn.Equals`, delta pairing, `ColumnFor` — and for
anything Weasel created itself the catalog already reports lowercase, so the only behaviour that
changes is the identifier the actual side renders.

If you call `Table.FetchExistingAsync` yourself and depended on lowercase column names coming back
from a table with quoted PascalCase columns, they now come back as the database spells them. Compare
with `StringComparer.OrdinalIgnoreCase`, or call `ColumnFor`, which already does.

PostgreSQL only. SQL Server and MySQL never folded introspected column names. Oracle and SQLite fold
the same way and are **not** fixed here: SQLite's identifiers are case-insensitive so the statement
works anyway, and Oracle's `TableColumn` ties case preservation to quoting in a way that needs its
own change.

::: tip A table holding both spellings was never mis-migrated
The issue suspected that a table with both `"Total_Amount"` and `total_amount` could have the wrong
column dropped. It cannot, and could not before this fix either. Name pairing is deliberately
case-insensitive ([#224](https://github.com/JasperFx/weasel/issues/224) — PostgreSQL folds unquoted
identifiers, so nothing downstream can tell `"Id"` from `id`), which means such a table is refused
outright with an `ArgumentException` rather than migrated wrongly. Unchanged, and now covered by a
test.
:::

### New public API in 9.35.1

- `ITable.AddOnlyMigrations`, implemented by `TableBase` and honored by all five providers' table
  deltas. Set by `MapToTable`; false everywhere else.
- `EfSchemaMappingCustomization.AllowDrops`, which clears it for every table of a mapping pass.
- `ISchemaObjectDeltaWithWithheldDrops`, implemented by every provider's `TableDelta`, exposing
  `WithheldDrops` as one entry per object left in place (`"column Total_Amount"`).
- `IMigrationLogger.WithheldDrop(string)`, a defaulted interface member that writes to the console,
  overridden by `DefaultMigrationLogger` to follow its writer.
- `AddOnlyMigration.DeclaredOnly<T>` and `AddOnlyMigration.Describe`, the shared filter and message
  the five providers use, so the policy reads the same on each.
- `Migrator.DefaultJsonColumnType`, `public virtual`, returning `"jsonb"` on the base and each
  engine's JSON store type on the provider migrators.

### Test infrastructure

`Weasel.EntityFrameworkCore.Tests`'s PostgreSQL connection string was a hard-coded `const` that no
environment variable reached, so that suite could only be run against `localhost:5432/marten_testing`.
It now honors `weasel_postgresql_testing_database` like every other Weasel suite, and like the SQL
Server half has since [#620](https://github.com/JasperFx/weasel/issues/620). No product change; it is
part of why a defect on a path that suite covers went unnoticed.

## 9.35.2

One fix, PostgreSQL only: [#634](https://github.com/JasperFx/weasel/issues/634).

`SchemaUtils.DropSchema(connectionString, schemaName)` retried the drop up to three times and, on the
third failure, **returned as though it had succeeded**. One condition served two opposite outcomes:

```csharp
if (success || ++reconnectionCount == maxReconnectionCount)
    return;
```

so the `throw` below the loop was unreachable, and the loop's own
`reconnectionCount < maxReconnectionCount` could never be false either.

`dropSchema` reports failure for exactly one condition — `57P01 admin_shutdown` — and rethrows
everything else. So the only way to exhaust the attempts is a server that is still down after the
backoff, which is precisely the case the caller needs to hear about. It heard nothing, and carried on
as though the schema were gone.

Exhausting the attempts now throws, which is what the unreachable line always intended:

```
System.InvalidOperationException: Unable to drop schema: my_schema
```

### Is this a behaviour change for you?

Only if a drop was already failing silently.

- A drop that succeeds, on any of the three attempts, behaves exactly as before.
- An exception that is not `admin_shutdown` propagates unretried and unwrapped, as before.
- A drop that exhausted all three attempts used to return normally and now throws. If you have a
  `try`/`catch` around this call that never fired, it may start firing — and what it is telling you is
  that the drop was not happening.

No signature changed. The public two-argument method drops its own `async` keyword and delegates to a
new internal overload that takes the single attempt as a delegate — an implementation detail, and
source- and binary-compatible. That overload is what makes the exhausted path testable without a
PostgreSQL server that stays down across three tries, which is why the defect went uncovered.
