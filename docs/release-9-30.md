# Upgrading to 9.30

9.30 is **the same code as 9.29.1**, published under a minor version because one of its three fixes
changes what an existing database does on its next migration. That belonged behind a minor bump
rather than a patch, and 9.29.1 got the number wrong.

::: tip Already on 9.29.1?
There is nothing to do beyond changing the number. The assemblies are identical — same commit, same
three fixes. Move to 9.30.0 so the version communicates what it should, and read the warning below
if you have not already.
:::

::: warning Character column widths are compared now
On MySQL, SQL Server and Oracle, a `varchar` whose width differs between your model and the database
now produces an `ALTER` where it previously produced nothing — **in both directions**.
[Read this before upgrading](#a-widened-character-column-is-detected).
:::

Three fixes. Two are silent false negatives — a schema that had drifted reported itself in sync —
and the third is a model-corruption bug in the SQLite rebuild.

## A widened character column is detected

[#550](https://github.com/JasperFx/weasel/pull/550), for
[JasperFx/wolverine#4246](https://github.com/JasperFx/wolverine/issues/4246).

`TableColumn.RawType()` strips the parenthesised part of a type before comparing, on MySQL, SQL
Server and Oracle alike. For most types that is right: MySQL 8 reports a bare `INT` for a column
declared `int(11)`, a `DECIMAL` carries a precision and a scale, a `DATETIME` a fractional-seconds
precision. Comparing those wholesale drifts on every schema check for tables nobody touched.

Character and binary lengths are the exception. They are declared by the model, reported faithfully
by every catalog, and load-bearing: a column narrower than the value being written fails the insert.
Widening a `varchar` in a model was invisible to the differ, so an existing database kept the old
width forever and only a hand-written `ALTER` fixed it. Downstream, a MySQL `varchar(255)` that
should have been wider failed node-record inserts with `Data too long for column 'description'`, and
widening the model was not enough on its own.

A length is compared only for the types whose single parenthesised argument really is a character or
byte count — `CHAR`, `VARCHAR`, `NCHAR`, `NVARCHAR`, `CHARACTER`, `VARCHAR2`, `NVARCHAR2`, `BINARY`,
`VARBINARY`, `RAW` — and only when both sides state one. "Cannot tell" never reports drift, so a
model that declares a bare type keeps comparing the way it always has. `varchar(max)` is an
unbounded sentinel, and Oracle's `VARCHAR2(100 CHAR)` and `(100 BYTE)` both read as 100.

**This is a real change in what an existing database does on its next migration.** A model
*narrower* than the existing column now emits a narrowing `ALTER`, which can fail on real data. That
is the correct contract — the model is the truth, and the differ makes the database match it — and
it is exactly what makes the downstream fix work, but it is worth checking your models against a
production catalog before the first migration on 9.29.1.

PostgreSQL and SQLite are deliberately untouched. PostgreSQL spells the type `character varying`,
which is not in the set, so its comparison is unchanged; SQLite is effectively typeless for this.

Two Oracle defects in `AlterColumnTypeSql` surfaced with it, neither reachable before, because a
column type delta was almost never detected in the first place. It emitted the shape the column was
moving *from* where the SQL Server and PostgreSQL twins emit the one it is moving *to*, so the
statement altered the column to what it already was; and it restated the column's nullability, which
Oracle rejects when it is not changing (ORA-01451, ORA-01442).

## A view body is compared without normalizing away its string literals

[#548](https://github.com/JasperFx/weasel/pull/548).

SQL Server and SQLite normalized a view body by stripping every whitespace character and folding
case across the whole string, literals included. Two views whose only difference was inside a
literal therefore compared equal: changing `where name = 'active'` to `'ACTIVE'`, or `'a b'` to
`'ab'`, both reported the schema as in sync. The view kept matching the old rows, the migration
never ran, and nothing gave a caller a way to notice.

Normalization now runs a scanner — `Weasel.Core.ViewSqlNormalizer` — that folds whitespace and case
only outside string literals and copies literal contents verbatim, doubled-quote escape included.
Delimited identifiers and comments are scanned as themselves, so the apostrophe in
`[Customer's Name]`, `"o'brien"`, `` `o'brien` `` or `-- don't` cannot open a literal and invert
inside/outside for the rest of the body.

Reformatting outside a literal remains not-drift, so **no existing schema starts reporting a
spurious migration**. Whitespace and case are still folded inside delimited identifiers, so
`"my column"` and `"mycolumn"` still compare equal — the same class of false negative, left in place
to keep everything outside literals behaviour-neutral. Correcting that carries its own
upgrade-migration question and belongs in a change of its own.

## A SQLite rebuild no longer mutates the model it was given

[#549](https://github.com/JasperFx/weasel/pull/549).

`writeTableRecreation` and its rollback twin built the throwaway replacement table out of the
caller's own `TableColumn` instances. `Table.AddColumn(TableColumn)` sets `column.Parent = this`, so
*generating* a migration silently reparented a long-lived model's columns onto a temp table
discarded a few lines later.

`Parent` is load-bearing: `TableColumn.DdlType` and the comparison type behind `Equals` both read
`Parent is { StrictTypes: true }`. No rebuild Weasel emits is affected — each temp table copies
`StrictTypes` from the table the columns came from, so the stale parent answers the same. The damage
lands afterwards, on the model that outlives the migration: set `StrictTypes` on it and its columns
consult the discarded parent, so the next diff reports drift on a column that already matches and
the `CREATE` fails with `unknown datatype for ...`.

Each column is cloned instead. The fix sits at the two call sites rather than in `AddColumn`, which
is public API and right to parent a column a caller hands it. The rebuild DDL is unchanged.
