# Upgrading to 9.39

9.39 adds an eighth provider — **Weasel.Firebird**, for Firebird 3, 4 and 5 — and fixes a SQL Server
migration that could not add a column and an expression over it in the same breath. If you are on
SQL Server and have ever seen `Invalid column name` out of a migration, that is this.

::: tip Coming from 9.38?
One change in rendered output: a SQL Server table delta that **changes a column** now carries `GO`
batch separators. Weasel's own executors and sqlcmd both split on them, so nothing in the normal
path changes. If you take `TableDelta.WriteUpdate` output and hand it to a single `SqlCommand`
yourself, split it first — `SqlServerBatchSplitter.Split(sql)` is public and does exactly that. This
is the same adjustment stored procedure DDL needed in 9.35.
:::

::: warning Coming from 9.37 or earlier?
Read [9.38](/release-9-38) as well. It shipped to NuGet without notes, and it carries a deliberate
change to what an Oracle migration reports about identity columns.
:::

## ⚠️ SQL Server: adding a column and an expression over it failed, and rolled nothing back

[#669](https://github.com/JasperFx/weasel/pull/669), from
[#668](https://github.com/JasperFx/weasel/issues/668) reported by
[@peejayess](https://github.com/peejayess) through Wolverine.

SQL Server compiles a whole batch before running any of it, and binds column names against tables
that already exist at compile time — deferred name resolution covers only tables that do not exist
yet. `TableDelta.WriteUpdate` wrote the missing columns' `ALTER TABLE … ADD` and everything after it
into one batch, so a later statement whose expression named a column the delta was adding failed to
compile with error 207:

```
alter table dbo.wolverine_dead_letters add expires datetimeoffset NULL;
CREATE INDEX idx_expires ON dbo.wolverine_dead_letters (expires) WHERE ([expires] IS NOT NULL);
-- Invalid column name 'expires'.
```

**The `ALTER` never ran either**, because nothing in a batch that did not compile does. A table being
created for the first time was never affected: the column is created with the table.

### The line is expression versus name list

This arrived as a report about an index, and it is not about indexes. What the server binds at
compile time is **expressions**:

| In the same batch as the column add | |
|---|---|
| a filtered index's predicate | failed |
| a check constraint | failed |
| a computed column derived from the new column | failed |
| an index's key columns | always worked |
| an index's `INCLUDE` list | always worked |
| a foreign key's columns | always worked |

So a **plain** index on a newly added column worked fine, which is exactly why this went unnoticed
for so long — and why the two cases beyond the original report were found only by measuring the
whole matrix. Only the first three ever failed, and all three are fixed.

`WriteUpdate` and `WriteRollback` now end the batch after every statement that adds, re-adds or
retypes a column. After *every* one, not once after the pass: a computed column can be derived from
another column the same delta is adding, so two statements inside the missing-columns pass alone are
enough to hit this.

The separator goes into the rendered text rather than into the executor, because that fixes both
paths at once. Every runtime path already splits on `GO`, and so do sqlcmd and SSMS — and nothing
but text would help a generated migration script.

## SQL Server: a rollback now undoes check-constraint changes

[#671](https://github.com/JasperFx/weasel/pull/671), from
[#670](https://github.com/JasperFx/weasel/issues/670).

`WriteUpdate` adds the check constraints the model declares and replaces the expressions that
drifted. `WriteRollback` had no counterpart at all, so a rolled-back migration left a constraint it
had added still on the table, and a constraint whose expression it had replaced still carrying the
new expression. The second is the worse half: the original expression was simply gone.

**If you have rolled back a SQL Server migration that touched a check constraint, the constraint on
the table is not what your model says it is, and no delta will tell you.** SQL Server's comparison
deliberately never treats an actual check constraint the expected table does not declare as an extra
to drop, so a constraint a rollback left behind is undeclared by definition and invisible to
`FindDeltaAsync`. Read `sys.check_constraints` if you need to check.

The drops run before any column change, because a check constraint is exactly what SQL Server
refuses to drop a column for. The adds run after the column passes, because an actual expression may
name a column only the actual table has — one the rollback itself re-adds.

## New: Weasel.Firebird

[#666](https://github.com/JasperFx/weasel/pull/666), contributed by
[@lahma](https://github.com/lahma).

`Weasel.Firebird` brings Firebird 3, 4 and 5 to parity with Weasel.MySql: tables with primary keys,
foreign keys (deferred cycles included), indexes, identity and computed columns, plus sequences,
views, PSQL functions, stored procedures and triggers, EF Core support, and CI on all three majors.

```bash
dotnet add package Weasel.Firebird
```

Firebird shapes most of the design, and it is worth knowing why the rendered DDL looks the way it
does:

- **It runs one statement per command**, so `FirebirdDbCommandBuilder` splits on `StartNewCommand`
  the way Oracle's builder does, and hands back one command per statement.
- **It has no schemas before Firebird 6**, so an object name is the name alone and the database is
  the schema. `DropSchemaAsync` empties the database rather than dropping it.
- **It has no `IF NOT EXISTS`**, and a racing DDL statement can lose, so every `CREATE` and `ADD` is
  a guarded `EXECUTE BLOCK` and every statement runs in its own `WAIT` transaction, re-run once
  after a lost catalog race.
- **It refuses to drop what a view or routine still uses**, so rollbacks run last delta first.

Rendered scripts are isql form, with a `COMMIT;` after every statement — isql runs a script in one
transaction until it meets a `COMMIT`, and Firebird applies DDL at commit, so a guarded
`EXECUTE BLOCK` and a plain statement sharing a transaction would fail together at that commit and
lose the guarded object silently.

### Two Core hooks came with it

Both are virtual with the previous behaviour as the default, so no other provider changes:

- `Migrator.FingerprintTableName(string)` — how a schema-fingerprint table is named. A provider with
  no schema to qualify a table with names the table alone.
- `Migrator.OrderRollbacks(IReadOnlyList<ISchemaObjectDelta>)` — the order a migration's deltas are
  undone in. The base keeps the order they were applied in, which suits a database that cascades a
  drop; a database that refuses to drop an object another still uses overrides it.

`DatabaseBase.ToDatabaseScript` also now writes through the writer `WriteScript` hands its step,
rather than the outer one it had captured. Every in-tree migrator passes that writer straight
through, so no existing script changes.

## Also in this release

- EF Core introspection goes through the migrator's command builder, which is what lets it work on a
  provider that runs one statement per command. This un-skips Oracle's EF `end_to_end` test.

## New public API

- The `Weasel.Firebird` package in full.
- `Weasel.SqlServer.SqlServerBatchSplitter.Separator` — the `GO` the splitter splits on, for a DDL
  writer that has to end a batch deliberately.
- `Migrator.FingerprintTableName` and `Migrator.OrderRollbacks`, described above.
