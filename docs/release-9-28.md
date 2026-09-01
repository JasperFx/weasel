# Upgrading to 9.28

9.28 is a SQLite release. Five changes, and unlike 9.27 — which was nine cases of Weasel *reading*
a schema back wrong — four of these change the DDL Weasel **emits**. A SQLite database created by
9.28 does not have the same column types as one created by 9.27.

::: danger The first migration after upgrading can throw
If any table declares a column as `numeric` or `decimal`, the first migration after upgrading fails
with a `SchemaMigrationException` on every `AutoCreate` setting except `AutoCreate.All`. The change
is safe and the rebuild that applies it preserves your data — it is the guard in front of it that
refuses. [Read this before upgrading](#the-first-migration-can-throw).
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

That difference is applied by rebuilding the table and copying every row, and that path works — but
`SchemaMigration.AssertPatchingIsValid` rejects `Invalid` on anything except `AutoCreate.All`,
regardless of whether a rebuild could apply it. So the migration that would have succeeded never
runs, and you get a `Weasel.Core.SchemaMigrationException` reading
`Cannot derive schema migrations for … AutoCreate.CreateOrUpdate`.

**Upgrading.** One of:

- Run the first migration with `AutoCreate.All`. The rebuild applies, data is copied, and every
  subsequent run is clean — there is nothing left to do on the second migration.
- Declare the column as `real` in your model if `REAL` is genuinely what you want. Nothing changes
  and no migration is needed.
- Stay on 9.27 until the guard is fixed.

This is a gap in the guard rather than in the change, and it affects any SQLite column-type change,
not only this one. It is tracked as a follow-up.

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
