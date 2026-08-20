# Upgrading to 9.25

9.25 is the identifier release. Nothing was rewritten, but **the DDL Weasel emits changes** for
any schema whose names are not all plain lowercase identifiers, and three behaviours change in
ways you can observe from the outside.

::: danger Go straight to 9.25.1
**9.25.0 refuses to create a table whose primary key constraint name is longer than the engine's
identifier limit**, and 9.24 accepted it. The names it rejects are entirely conventional — just
long, which a wide composite key on a long table name produces easily. Fixed in 9.25.1; see
[the 9.25.1 fix](#what-9-25-1-fixes) below.
:::

If your schema uses conventional names throughout — lowercase, no spaces, no reserved words, no
brackets — the emitted DDL is byte-identical to 9.24 **with the one exception above**, and there is
nothing else here to do except read
[the one that looks like a regression](#the-one-that-looks-like-a-regression).

## What 9.25.1 fixes

[#485](https://github.com/JasperFx/weasel/issues/485) and
[#486](https://github.com/JasperFx/weasel/issues/486) — one root cause, two symptoms.

9.25.0 started validating the names a table writes that are not database objects of their own —
its columns, its primary key constraint, its check constraints
([#468](https://github.com/JasperFx/weasel/pull/468)). It ran them through the provider's
`AssertValidIdentifier`, which also enforces the **length** limit. Schemas that applied cleanly on
9.24 began throwing.

The length rule does not belong there, and Weasel's own code already said so. An *object* name the
database truncates becomes unaddressable and drifts on every check afterwards — worth refusing. A
*local* identifier is only ever emitted inside its own table's DDL and never addressed by name
again, and `TableDelta` already compares both `PrimaryKeyName` and the primary key column list
through `TruncatedNameIdentifier` **precisely so a truncated one still matches**. Weasel was
handling truncated local identifiers downstream while refusing to create them upstream.

`Migrator.AssertValidLocalIdentifier` now applies the same safety rules with no length limit. A
quote, a semicolon, a line break, leading or trailing whitespace are still rejected wherever they
appear. The base implementation defers to `AssertValidIdentifier`, so a provider outside this
repository keeps the stricter behaviour until it opts in.

::: warning The quiet half
#486 presented as a `23505` on `pk_mt_event_progression` during a Marten per-tenant daemon
catch-up — an error naming a table with nothing to do with identifiers. The rejection had aborted
a projection's schema application, and the daemon then ran against storage that was never created.
**The loud failure and the quiet one were the same rejection.** If you saw anything strange on
9.25.0, this is worth ruling out before looking further.
:::

Also in 9.25.1: **functions on MySQL and Oracle**
([#482](https://github.com/JasperFx/weasel/issues/482)), which completes the object type matrix
apart from check constraints on Oracle, MySQL and SQLite
([#488](https://github.com/JasperFx/weasel/issues/488) — a pre-existing gap, not a 9.25 change).

## At a glance

| Change | Who is affected | Action |
| --- | --- | --- |
| **SQLite no longer drops your table to change a column** | Anyone on SQLite applying a type, foreign key or primary key change with `AutoCreate.All` | None — but read below, because you may have lost data on an earlier version |
| **Oracle now sees index, foreign key and primary key drift** | Every Oracle user | Expect a first run that finally applies indexes it had silently been ignoring |
| Column names are no longer rewritten | Anyone who declared a column with a space in the name | Write the underscore yourself, or accept the new name |
| SQL Server quotes identifiers it previously left bare | Names that are not regular identifiers | None — the DDL was invalid before |
| A name you bracketed yourself is passed through and unbracketed in the model | SQL Server callers who bracketed to work around the above | Stop bracketing; compare against the bare name |
| MySQL `Table.Identifier` renders and compares differently | Anyone comparing `DbObjectName` instances directly | Compare through `MySqlObjectName` |
| `Weasel.MySql.Sequence` is obsolete | Anyone using it — though nothing could consume it | Use `AUTO_INCREMENT` |
| Schema fingerprints re-evaluate once | Anyone using `Migrator.UseSchemaFingerprinting` | None — expect one "everything changed" pass |

## ⚠️ Column names are no longer rewritten

**[#458](https://github.com/JasperFx/weasel/issues/458).** PostgreSQL, Oracle, SQLite and SQL
Server used to turn a space in a column name into an underscore. They no longer do.

```csharp
table.AddColumn("Order Date", "datetime2");
// 9.24: creates Order_Date
// 9.25: creates "Order Date"
```

**On an existing database this reads as a new column**, so a migration will try to add
`"Order Date"` alongside the `Order_Date` that is already there. If you were relying on the
rewrite, write the underscore yourself:

```csharp
table.AddColumn("Order_Date", "datetime2");   // unchanged on both versions
```

Why it changed: the rewrite only ever happened in `TableColumn`. Index, foreign key and primary
key column lists were left alone, so declaring `Order Date` produced an index over a column that
did not exist — no error at model time, and either a failed statement at the server or an index
that drifted forever. The rewrite was also never a decision anyone made; it predates the
identifier work, and [#448](https://github.com/JasperFx/weasel/issues/448) settled that a
supplied name is honoured or rejected, never silently changed.

`PreserveIdentifierCase` now controls case folding and nothing else — it used to switch off the
space rewrite as a side effect. SQLite gains the flag, which it was the only provider to lack.

## SQL Server: identifiers are quoted where they were not

**[#443](https://github.com/JasperFx/weasel/pull/443), [#446](https://github.com/JasperFx/weasel/pull/446).**
`SchemaUtils.QuoteName` bracketed only a hardcoded reserved-word list, and several emission sites
did not call it at all. Ordinary schemas are byte-identical; any name that is not a regular
identifier now brackets.

**A name you bracketed yourself is now honoured, and unbracketed on the way into the model.**
Because Weasel emitted most identifiers bare, bracketing the name yourself was the only way to
use one that needed delimiting. That still works:

```csharp
table.AddColumn("[Order Date]", "datetime2");   // names the column Order Date
```

But `IndexDefinition.Name`, `TableColumn.Name`, `ForeignKey.Name`, `PrimaryKeyName` and the
index and FK column lists now all hold the **bare** name. If you compare against those, compare
against `Order Date`, not `[Order Date]`.

**Consequence:** a name that genuinely contains its own brackets can no longer be expressed —
`[x]` names the object `x`.

Smaller: check constraint names honour the same pass-through; `SqlServerObjectName` brackets a
name containing a `.`; `GenerateDeleteAllSql` emits bracketed names in its `DELETE` and
`DBCC CHECKIDENT` statements.

::: warning `SchemaUtils.QuoteColumnEntry` never shipped
It was added in #443 and removed in #446, both inside this release. It is not part of the API.
:::

## MySQL: identifiers are normalized into the model

**[#445](https://github.com/JasperFx/weasel/pull/445).**

`Table(DbObjectName)` now normalizes to `MySqlObjectName`, so
`Table.Identifier.QualifiedName` renders as `` `schema`.`name` `` rather than `schema.name`.
`Table(string)` already went through `MySqlProvider.Parse` and is unaffected.

**`table.Identifier` no longer compares equal to a plain `new DbObjectName(schema, name)`** for
the same table. That inequality was the bug — a hand-built identifier never matched the catalog,
so the foreign key reported drift on every check — but it is visible to anyone comparing
identifiers directly.

`ForeignKey.LinkedTable` is normalized too, so emitted `REFERENCES` clauses are backtick-quoted.

**An index may now be retained that previously drifted.** Where an index's leading columns cover
a surviving foreign key and the expected table does not declare it, one such index is held back
from the comparison so InnoDB does not refuse the `DROP`. Only the last remaining cover is kept.

## The one that looks like a regression

Emitted DDL text changes for MySQL and SQL Server schemas that were previously emitting unquoted
or invalid identifiers. **Anything that hashes DDL re-evaluates on the first run after the
upgrade** — in particular the schema fingerprint stamps behind
`Migrator.UseSchemaFingerprinting`.

That is a one-time "everything looks changed" pass, not drift. The second run is quiet.

## Two bugs that were losing work silently

Neither was reported by a user. Both were found by the parity work in
[#455](https://github.com/JasperFx/weasel/issues/455), and both had been failing quietly.

### SQLite dropped the table to change a column

[#477](https://github.com/JasperFx/weasel/issues/477). `TableDelta` has always had a careful
rebuild — create a new table, copy the surviving columns, drop the old one, rename. **The migrator
never called it.** A change needing a rebuild reports `Invalid`, and `Invalid` was answered by
dropping the object and creating it again.

Measured on a column type change: **one row before, zero after** — and a schema that looked
entirely correct afterwards.

Affects every SQLite change that `ALTER TABLE` cannot express: a column type change, a foreign key
added or removed, a primary key change. It needs `AutoCreate.All`, which is what a developer
resetting a local database and `db-apply` on an environment configured for it both use.

If you have applied such a change on 9.24 or earlier, the data is gone and this release cannot
bring it back. It will not happen again.

### Oracle could not see indexes, foreign keys or primary keys

[#474](https://github.com/JasperFx/weasel/issues/474). ODP.NET will not execute several statements
from one command, so an Oracle schema object could register exactly one introspection query — and
`Table` spent it on columns. Everything else was invisible to `SchemaMigration.DetermineAsync`,
which is what `ApplyChangesAsync`, `ApplyAllConfiguredChangesToDatabaseAsync` and
`AssertDatabaseMatchesConfigurationAsync` all go through.

In practice: a declared index was created with the table and never touched again. Adding one to an
existing table did nothing. Changing one did nothing. Removing one left it in place. And
`AssertDatabaseMatchesConfigurationAsync` reported a clean match throughout.

**Expect your first Oracle run on 9.25 to apply index and foreign key changes it had been
ignoring.** That is the backlog being worked off, not new drift.

## New in this release

| | |
| --- | --- |
| **Views on SQL Server, Oracle and MySQL** | [#450](https://github.com/JasperFx/weasel/issues/450). All five providers now model views. MySQL's is the interesting one — it rewrites a view's definition when it stores it, so Weasel canonicalizes your SQL through the server to compare. That needs CREATE VIEW permission on the assert path as well as the apply path; see [Object Type Support](/core/object-types). |
| **Triggers, on all five providers** | [#452](https://github.com/JasperFx/weasel/issues/452). The one whole category no provider modelled. They are independent schema objects that declare a target, not something a table owns — see [Triggers](/core/triggers). |
| **Stored procedures on PostgreSQL, MySQL and Oracle** | [#451](https://github.com/JasperFx/weasel/issues/451). SQL Server had the only implementation; it now derives from a shared `StoredProcedureBase`. See [Stored Procedures](/core/procedures). |
| **Oracle packages, materialized views and synonyms; SQL Server synonyms; PostgreSQL enums, domains and composites** | [#453](https://github.com/JasperFx/weasel/issues/453). |
| **A shared index scenario matrix** | [#449](https://github.com/JasperFx/weasel/issues/449). Eleven create-then-introspect scenarios run against every provider. It found three real defects on its first run, including that SQLite was comparing indexes by name alone. |
| **Every identifier a table writes is validated** | [#448](https://github.com/JasperFx/weasel/issues/448). Column names, the primary key constraint name and check constraint names were previously validated by no provider at all. |
| **An interior space is a legal identifier** | `unit price` passes validation. A leading or trailing space, a tab and a line break still do not. |
| **Oracle teardown drops what it used to leave behind** | [#465](https://github.com/JasperFx/weasel/issues/465). Triggers, packages, synonyms, object types and materialized views. |
| **MySQL has schema extensions** | `DropSchemaAsync`, `CreateSchemaAsync`, `ResetSchemaAsync` — it was the one provider of five without them. |

Two new reference pages: [Identifiers and Quoting](/core/identifiers) and
[Object Type Support](/core/object-types).

## Weasel.Core API additions

Additive, but on public abstract types that downstream providers derive from:

| Member | Why |
| --- | --- |
| `ISchemaObjectWithLocalIdentifiers` | Carries the names that are not database objects — columns, primary key, check constraints — so the migration path can validate them. `TableBase` implements it, so no provider had to. |
| `CommandBuilderBase.Command` | Exposes the command being built, so a provider's `ConfigureQueryCommand` can set driver options its introspection query depends on. Oracle needs it: `ALL_VIEWS.TEXT` is a LONG and ODP.NET reads it back empty by default. |
| `IdentifierRules` | The shared quoting contract. Each provider's static `SchemaUtils` delegates to it, so no DDL call site changed. |
| `TableBase.NormalizeIdentifier` | `protected virtual`, called from the `PrimaryKeyName` setter. A no-op by default; only SQL Server overrides it. |
| `TriggerBase` | The cross-provider trigger model. |
| `StoredProcedureBase` | The cross-provider stored procedure model, lifted from SQL Server's implementation. |
| `ISchemaObjectDeltaWithRebuild` | Lets a delta say "I cannot express this as an `ALTER`, but I can make the change without losing the data". An interface rather than a new `SchemaPatchDifference` value, because the enum is public and consumers switch on it. |
| `IBatchedCommandBuilder`, `SchemaObjectBase.CreateCommandBuilder` | Let a schema object register more than one introspection query on a provider whose driver executes one statement per command. Oracle is the only such provider. |

## Refused rather than ignored

Four properties that used to accept a value and silently do nothing now throw, naming the
supported alternative. A caller gets what they set, or an exception — never a quietly narrower
object.

| Property | Why |
| --- | --- |
| `IndexDefinition.Predicate` on MySQL and Oracle | Neither engine has partial indexes. It was never emitted and never compared. |
| `Sequence.IncrementBy` on MySQL | The table-based emulation never honoured it. |
| `Trigger.Condition` on SQL Server and MySQL | Neither has a `WHEN` clause. |
| `Trigger.Timing = Before` on SQL Server | SQL Server has no `BEFORE` trigger. |
