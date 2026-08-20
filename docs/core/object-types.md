# Object Type Support

Which database object types Weasel can create, compare and drop, on each of the five
providers. This is the companion to the [Provider Trait Matrix](/core/provider-trait-matrix),
which covers behaviour rather than object types.

Three symbols, and the distinction between the last two matters:

| Symbol | Meaning |
| --- | --- |
| ✓ | Weasel models it: `WriteCreateStatement`, delta detection, and teardown |
| ✗ | The engine has it, Weasel does not model it yet — an open issue, not a decision |
| — | Not a concept in this engine, so there is nothing to model |

## The matrix

| Object type | PostgreSQL | SQL Server | Oracle | MySQL | SQLite |
| --- | --- | --- | --- | --- | --- |
| Table | ✓ | ✓ | ✓ | ✓ | ✓ |
| Index | ✓ | ✓ | ✓ | ✓ | ✓ |
| Foreign key | ✓ | ✓ | ✓ | ✓ | ✓ (inline only) |
| Check constraint | ✓ | ✓ | ✗ | ✗ | ✗ |
| Primary key | ✓ | ✓ | ✓ | ✓ | ✓ |
| Sequence | ✓ | ✓ | ✓ | — (obsolete emulation) | — |
| View | ✓ | ✓ | ✓ | ✓ | ✓ |
| Materialized view | ✓ | — | ✓ | — | — |
| Function | ✓ | ✓ | ✗ | ✗ | connection-scoped |
| Stored procedure | ✓ | ✓ | ✓ | ✓ | — |
| Trigger | ✓ | ✓ | ✓ | ✓ | ✓ |
| Package | — | — | ✓ | — | — |
| Synonym | — | ✓ | ✓ | — | — |
| User-defined type | ✓ (enum/domain/composite) | ✓ (table types) | — | — | — |
| Extension | ✓ | — | — | — | — |
| Partitioning | ✓ (hash/range/list) | ✓ (range) | ✓ | ✓ | — |

### Reading the gaps

- **Triggers** are modelled as independent schema objects that declare a target rather than as something a table owns, which is why they are a row here and not part of the table row. A trigger does not always have a table — SQL Server's `INSTEAD OF` triggers attach to views — and PostgreSQL's call a function, so they compose with `Function` rather than carrying their own body. See [Triggers](/core/triggers).
- **Functions on Oracle and MySQL** are [#450](https://github.com/JasperFx/weasel/issues/450). Stored procedures are on all four engines that have them; SQLite has no such concept.
- **Oracle packages** model the specification and the body as one object with two parts, because that is what they are: `all_source` lists them separately, they compile separately, and a body can be invalid while its spec is fine. A spec-only package — shared constants and types — is legal and supported.
- **PostgreSQL user-defined types** cover enums, domains and composites through one class, because the catalog makes no distinction between them and neither does anything Weasel does with them.
- **PostgreSQL materialized views already work**, through `Weasel.Postgresql.Views.MaterializedView`, which is `View` with a different `ViewType` and an optional access method. That row was ✗ in the first draft of this page and the check below caught it on its first run, which is the argument for the check.

### SQLite functions are a different thing

SQLite has no stored function objects. Functions are registered against a *connection* through
`Microsoft.Data.Sqlite` and vanish when it closes, so there is nothing in the database for a
migration to create or a delta to compare. `Weasel.Sqlite.Functions` exists to make registering
them repeatable, not to model a schema object.

### Sequences on MySQL and SQLite

Neither engine has a native `SEQUENCE` — MySQL never has, at any version; it is MariaDB 10.3 that
added them.

**`Weasel.MySql.Sequence` is `[Obsolete]`.** It emulates a sequence with a single-row table, and
nothing can consume it: `current_value` is never read or incremented, there is no next-value
operation anywhere, and its delta only checks existence. Use `AUTO_INCREMENT`.

That follows the rule [#453](https://github.com/JasperFx/weasel/issues/453) settled: **emulate
operations, not objects.** SQLite's table recreation around its `ALTER` limits and MySQL's DDL
ordering around foreign-key backing indexes are fine, because the end state is the object the
caller declared and the emulation is invisible. An emulated *object* makes `AllObjects()` and
introspection lie, and the semantics diverge where it matters — a real sequence does not roll back
and does not serialize writers; a table-backed counter does both. Which is why SQLite does not get
one either.

## Teardown has to keep up

`DropSchemaAsync` splits two ways, and only one of them is safe against additions:

| Provider | Strategy | Can it fall behind? |
| --- | --- | --- |
| PostgreSQL | `DROP SCHEMA … CASCADE` | No — the server cascades |
| MySQL | `DROP DATABASE IF EXISTS` | No — the server cascades |
| SQLite | enumerates `sqlite_master` | Yes |
| SQL Server | enumerates `information_schema` / `sys` | Yes |
| Oracle | enumerates `all_*` | Yes |

The three that enumerate have to learn about every new object type, and they do not fail loudly
when they have not — the object simply survives, and the next thing that needs an empty schema
breaks instead. SQL Server's teardown was silently broken for views for exactly as long as
nothing could create one ([#464](https://github.com/JasperFx/weasel/pull/464)); Oracle's was the
same trap, found before it was armed ([#465](https://github.com/JasperFx/weasel/issues/465)).

::: warning Oracle's `DropSchemaAsync` empties the schema, it does not drop it
An Oracle schema is a user, and the only statement that drops one is `DROP USER … CASCADE` —
which a session cannot run against its own user (ORA-01940). The name is kept for symmetry with
the other four providers.
:::

## This page is checked against the code

A matrix like this is worth nothing once it drifts, so
`Weasel.Core.Tests.object_type_support_matrix` reflects over the five provider assemblies and
asserts that every ✓ above corresponds to a type that really implements `ISchemaObject`, and
that every ✗ really is absent. A row that goes stale fails the build.
