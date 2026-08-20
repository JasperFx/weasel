# Upgrading to 9.25

9.25 is the identifier release. Nothing was rewritten, but **the DDL Weasel emits changes** for
any schema whose names are not all plain lowercase identifiers, and three behaviours change in
ways you can observe from the outside.

If your schema uses conventional names throughout — lowercase, no spaces, no reserved words, no
brackets — the emitted DDL is byte-identical to 9.24 and there is nothing here to do except read
[the one that looks like a regression](#the-one-that-looks-like-a-regression).

## At a glance

| Change | Who is affected | Action |
| --- | --- | --- |
| Column names are no longer rewritten | Anyone who declared a column with a space in the name | Write the underscore yourself, or accept the new name |
| SQL Server quotes identifiers it previously left bare | Names that are not regular identifiers | None — the DDL was invalid before |
| A name you bracketed yourself is passed through and unbracketed in the model | SQL Server callers who bracketed to work around the above | Stop bracketing; compare against the bare name |
| MySQL `Table.Identifier` renders and compares differently | Anyone comparing `DbObjectName` instances directly | Compare through `MySqlObjectName` |
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

## New in this release

| | |
| --- | --- |
| **Views on SQL Server, Oracle and MySQL** | [#450](https://github.com/JasperFx/weasel/issues/450). All five providers now model views. MySQL's is the interesting one — it rewrites a view's definition when it stores it, so Weasel canonicalizes your SQL through the server to compare. That needs CREATE VIEW permission on the assert path as well as the apply path; see [Object Type Support](/core/object-types). |
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
