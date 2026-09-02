# Upgrading to 9.28

9.28 is a SQLite release. Five changes, and unlike 9.27 — which was nine cases of Weasel *reading*
a schema back wrong — four of these change the DDL Weasel **emits**. A SQLite database created by
9.28 does not have the same column types as one created by 9.27.

::: danger The first migration on 9.28.0 can throw — fixed in 9.28.1
If any table declares a column as `numeric` or `decimal`, the first migration after upgrading to
**9.28.0** fails with a `SchemaMigrationException` on every `AutoCreate` setting except
`AutoCreate.All`. The change is safe and the rebuild that applies it preserves your data — it was
the guard in front of it that refused. **9.28.1 fixes the guard**; go straight to it and there is
nothing to do. [Read this if you are on 9.28.0](#the-first-migration-can-throw).
:::

::: warning SQLite column types are what you declared them to be now
Weasel used to collapse every declared type onto one of SQLite's five storage classes. It no longer
does. A model that says `TIMESTAMP` now creates a `TIMESTAMP` column rather than a `TEXT` one.
Existing tables are not rebuilt for this, but a table rebuilt for any other reason comes back with
its declared types.
:::

## The first migration can throw

[#533](https://github.com/JasperFx/weasel/pull/533) changes what `numeric` and `decimal` map to, so
a table Weasel created before 9.28 has a `REAL` column where the model now asks for `NUMERIC`.
SQLite cannot express a column type change as an `ALTER`, so the delta comes back as
`SchemaPatchDifference.Invalid`.

That difference is applied by rebuilding the table and copying every row, and that path works. On
9.28.0 the guard above it did not agree: `SchemaMigration.AssertPatchingIsValid` rejected `Invalid`
on anything except `AutoCreate.All`, regardless of whether a rebuild could apply it. So the
migration that would have succeeded never ran, and you got a `Weasel.Core.SchemaMigrationException`
reading `Cannot derive schema migrations for … AutoCreate.CreateOrUpdate`.

**[#538](https://github.com/JasperFx/weasel/issues/538) fixes this in 9.28.1.** The guard now asks
the delta whether it can rebuild in place — the same question both apply paths below it already
ask — and lets it through under `AutoCreate.CreateOrUpdate` when it can. `AutoCreate.CreateOnly`
still refuses, because a rebuild recreates a table that is already there and that is an update.

**Upgrading.**

- **From 9.27 or earlier:** upgrade to 9.28.1 rather than 9.28.0 and there is nothing to do. The
  first migration rebuilds the table, copies every row, and converges.
- **Already on 9.28.0:** upgrade to 9.28.1, or run the first migration once with `AutoCreate.All`.
  Either applies the same rebuild.
- Declaring the column as `real` in your model also works if `REAL` is genuinely what you want —
  nothing changes and no migration is needed.

Note that the gap was never specific to `numeric`: it affected **any** SQLite column type change, as
well as adding or dropping a foreign key and changing a primary key. The `numeric` change is only
what made it reachable for people who had not changed their model at all.

## What changed

### numeric and decimal have NUMERIC affinity, not REAL

[#533](https://github.com/JasperFx/weasel/pull/533). SQLite gives a declared type REAL affinity only
when it contains `REAL`, `FLOA` or `DOUB`. `NUMERIC` and `DECIMAL` get NUMERIC affinity, and the two
store data differently: NUMERIC keeps a whole number as an integer, REAL converts it to a float. An
id declared `numeric` came back as `1.0` rather than `1`.

Note that this applies to a type declared as a **string**. `AddColumn<decimal>` is unchanged and
still maps to `REAL`.

### A column keeps the type it was declared with

[#532](https://github.com/JasperFx/weasel/pull/532). `TableColumn`'s constructor ran every type
through `ConvertSynonyms`, which collapses declared types onto SQLite's storage classes. SQLite
stores the declared text verbatim and derives affinity from it by substring rules, so this was not
cosmetic — a model asking for `TIMESTAMP` (NUMERIC affinity) got a `TEXT` column, and reading an
existing database back lost what that database actually said.

This is what makes Weasel usable against a SQLite database it did not create.

**Upgrading.** Existing tables are not rebuilt for this: comparison normalizes both sides, so a
column stored as `TEXT` and a model saying `DATETIME` still compare equal. But a table rebuilt for
some other reason is recreated from the model's declared types, and a value that parses as a number
— a bare `2024` in a `DATE` column, say — is copied into the new column as a number rather than as
text.

### STRICT tables accept the types they are given

[#535](https://github.com/JasperFx/weasel/pull/535). A STRICT table accepts only `INT`, `INTEGER`,
`REAL`, `TEXT`, `BLOB` and `ANY`. Declaring a column `numeric` or `decimal` on one failed outright:

```
SQLite Error 1: 'unknown datatype for t.quantity: "NUMERIC"'
```

`NUMERIC` now maps to `ANY` on a STRICT table — the one STRICT type that keeps a whole number an
integer rather than converting it to a float. A parameterized type such as `VARCHAR(255)` was
rejected the same way and for longer than the other two changes have existed; it now maps to `TEXT`.

### NOT NULL survives on a primary key column

[#534](https://github.com/JasperFx/weasel/pull/534). The column declaration suppressed `NOT NULL`
whenever it emitted an inline `PRIMARY KEY`, on the grounds that SQLite applies it implicitly. It
does not. Outside a `WITHOUT ROWID` table only `INTEGER PRIMARY KEY` is safe, and only because it is
a rowid alias whose NULL is replaced by the next rowid rather than stored. A `REAL` or `TEXT`
primary key stores the NULL, so a rebuilt table accepted keys the original rejected.

### A view body is split on its own AS

[#531](https://github.com/JasperFx/weasel/pull/531). **This one is not SQLite-only — it affects SQL
Server too.**

Both providers found a view's body by taking everything after the first `" AS "`. That is wrong
whenever the view's own `AS` stands on a line by itself, because the first match is then a column
alias inside the SELECT list:

```sql
CREATE VIEW foo
AS
SELECT 1 AS MyColumn
```

The body came back as `MyColumn`. It failed silently: both sides of a delta went through the same
extraction, so the truncation cancelled out and the comparison reported no drift, while the view
that would have been rebuilt was not valid SQL at all.

Extraction now lives in `Weasel.Core.ViewDefinition.ExtractBody` and finds the first bare `AS` that
is not inside parentheses, a comment, a string literal, or a delimited identifier. PostgreSQL and
Oracle are unaffected — their catalogs return the body already.

**Upgrading.** If you match existing views by declaring them in Weasel, a view whose stored text put
`AS` on its own line was being compared on a truncated body. It will now compare on the whole one,
which may surface a delta that was previously invisible.
