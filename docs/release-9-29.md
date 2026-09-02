# Upgrading to 9.29

9.29 clears 9.28's known break, makes the SQLite table rebuild safe to run against a database that
has foreign keys, and adds two capabilities: mutually referencing tables can be created from
scratch, and a PostgreSQL full text index can be built over a `tsvector` you assembled yourself.

::: tip 9.28's upgrade break is fixed
If you held off on 9.28 because a `numeric` or `decimal` SQLite column made the first migration
throw on anything but `AutoCreate.All`, that is fixed. Upgrade straight from 9.27 — you do not need
to pass through 9.28 or run anything with `AutoCreate.All`.
:::

::: warning A SQLite rebuild can now refuse, and a failed one no longer continues
Both are deliberate, and both can turn a migration that used to "succeed" into one that throws.
[Read this before upgrading](#the-sqlite-rebuild-is-atomic-now).
:::

## The guard in front of a rebuild no longer refuses it

[#542](https://github.com/JasperFx/weasel/pull/542), closing
[#538](https://github.com/JasperFx/weasel/issues/538).

`SchemaMigration.AssertPatchingIsValid` threw for `SchemaPatchDifference.Invalid` on every
`AutoCreate` except `All`, without asking whether the delta could actually apply the change. Both
apply paths below it *do* ask, and answer a rebuildable delta by rebuilding it and copying the data
rather than dropping and recreating. So the gate was stricter than the thing it gated, and refused
migrations the machinery would have carried out correctly.

`AutoCreate.CreateOrUpdate` now permits a rebuild. `CreateOnly` still refuses one, and needs no
special case to do it: the delta is still `Invalid`, so the migration's `Difference` is still not
`Create`.

This reverses a decision [#477](https://github.com/JasperFx/weasel/issues/477) shipped on purpose —
that a rebuild always needs `AutoCreate.All`, because a rebuild that also drops a column takes that
column's data with it. That reasoning did not survive contact with the code. `AssertPatchingIsValid`
could only express it by refusing *every* rebuildable delta, including the great majority that drop
nothing — a column type change, a foreign key, a primary key — while SQLite's ordinary `Update` path
was already emitting `ALTER TABLE … DROP COLUMN` under `CreateOrUpdate`. The strict rule only
refused the same loss when the column happened to sit in a key.

**A rebuild copies every row.** On a large table that is a very different proposition from an
`ALTER`, even though both are "an update". If that matters to you, `AutoCreate.CreateOnly` still
refuses.

## The SQLite rebuild is atomic now

[#539](https://github.com/JasperFx/weasel/pull/539).

SQLite cannot `ALTER` most of a table, so a change is applied by rebuilding it: create, copy, drop,
rename. Those four statements ran with no transaction, each one autocommitting.

Rebuilding a table that another table's foreign key references broke the database. The `DROP` failed
on its implicit delete, *after* the replacement had been created, filled and committed. The orphan
`_new` table survived, `CREATE TABLE IF NOT EXISTS` no-opped on it at the next start, and every run
after that failed differently until someone dropped it by hand. Foreign keys are on by default in
`Microsoft.Data.Sqlite` and in all three `SqlitePragmaSettings` presets, so this was the ordinary
case rather than an exotic one.

The rebuild now runs the way SQLite documents it: enforcement suspended, the whole thing in one
transaction, `foreign_key_check` before the commit.

**Two behaviour changes come with that.**

A rebuild that really would leave a dangling reference is now **refused** and rolled back, with an
`InvalidOperationException` naming the table. Previously it committed the damage. The check only
runs when foreign key enforcement was on to begin with — a database deliberately running with
`foreign_keys` OFF is allowed to hold dangling rows, and is not second-guessed.

A custom `IMigrationLogger` that declines to rethrow used to let a failed rebuild reach `COMMIT`
half-applied. It is still handed the failure through `OnFailure`, but the migration no longer
continues past it. If you have a logger that swallows, a migration that previously appeared to
succeed may now throw — it was not succeeding before.

Two smaller fixes ride along: an `AUTOINCREMENT` table came out of a rebuild with a lower
`sqlite_sequence` high-water mark than it went in with and reissued an id it had already handed out,
and a view over the rebuilt table failed the rename outright.

## Mutually referencing tables can be created from scratch

[#540](https://github.com/JasperFx/weasel/pull/540).

A table's foreign keys are written into its own create statement, so a key pointing at a table the
migration has not created yet references nothing. Two tables referencing each other could never be
created from scratch — neither could go first, the apply threw on the first `ALTER`, and every later
delta was abandoned, including the create of the table the key was waiting for. Re-running
reproduced the same failing statement, so it never recovered.

A migration now holds back exactly the keys that would fail — those whose referenced table is
created by a later delta — and applies them once every delta has run. Keys whose target already
exists, or is created earlier in the same migration, stay where they were, **so a schema that never
had the problem generates byte-for-byte identical DDL**. Nothing to do on upgrade.

Only SQL Server and PostgreSQL defer. SQLite writes its foreign keys inline in `CREATE TABLE` and
never had the problem.

::: warning Rolling one back is not symmetrical
`WriteAllRollbacks` answers a created table with a bare drop and knows nothing about deferred keys.
On SQL Server neither table of a cycle can be dropped while the other's key points at it, so the
rollback of a cycle-creating migration fails on the first `DROP`. PostgreSQL escapes this only
because its drop appends `CASCADE`. Tracked as a follow-up.
:::

## A full text index over a tsvector you built yourself

[#543](https://github.com/JasperFx/weasel/pull/543), closing
[#541](https://github.com/JasperFx/weasel/issues/541). PostgreSQL.

`FullTextIndexDefinition` wrapped whatever it was given in `to_tsvector`, so `DocumentConfig` was
always *text to be converted* and there was no way to hand it an expression that was already a
`tsvector`. That put per-member weighting out of reach, because `setweight` labels a vector — so
weighting works by converting each member separately and concatenating the **vectors**, not the
text.

`TsVectorExpression` is consumed without the wrapping:

```csharp
var index = FullTextIndexDefinition.ForTsVector(tableName,
    "setweight(to_tsvector('english', coalesce(data ->> 'Title', '')), 'A') || " +
    "setweight(to_tsvector('english', coalesce(data ->> 'Body', '')), 'B')");
```

Leave it unset — the default — and the definition behaves exactly as it always has, down to the
byte. That matters: a changed index expression makes Weasel drop and recreate the index, which on a
large table is an outage rather than a migration.

Read `IndexedTsVector` to find out what is actually indexed, whichever way it was configured. The
DDL is generated from that property and nothing else, so a consumer building the query-side filter
should read it from there too — a `ts_rank` computed over a different vector than the one `@@`
filtered on is silently wrong rather than merely slow.

## SQLite delete-all no longer clears the wrong schema's table

[#545](https://github.com/JasperFx/weasel/pull/545).

`GenerateDeleteAllSql` built every statement from the table name alone and threw the schema away, so
each one went out unqualified and SQLite resolved it by search order — which puts `temp` first. A
temp table silently took the delete meant for the main one, and `sqlite_sequence` is per-database
for the same reason.

::: warning Known issue
A schema with no `AUTOINCREMENT` table has no `sqlite_sequence`, and the identity reset now fails
against it with `no such table`. The single-schema form of this predates 9.29; the multi-schema form
is new, and is the price of the fix being correct. Tracked as
[#546](https://github.com/JasperFx/weasel/issues/546).

Calling `GenerateDeleteAllSql` yourself, you can pass `resetIdentity: false` to avoid it. Going
through `DatabaseCleaner` you cannot — it calls the two-argument overload with the default — so on
a schema with no `AUTOINCREMENT` table, prefer 9.28 for that path until #546 lands.
:::
