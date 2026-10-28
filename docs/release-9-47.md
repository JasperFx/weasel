# Upgrading to 9.47

One additive change, for event store dialects built on `Weasel.Storage`.

A dialect can now say that its per-event append SQL returns the `seq_id` the insert drew, and
`Weasel.Storage` will read that value back onto `IEvent.Sequence`.

**Nothing public was removed and no signature changed.** The two additions are `init` properties on
the Quick descriptor classes, and both default to `false` — a dialect that does not set the flag gets
exactly the operation it got in 9.46.

## A per-event Quick append can read back the sequence it drew

[#747](https://github.com/JasperFx/weasel/pull/747), contributed by
[@vpetrevski](https://github.com/vpetrevski), as the Weasel half of
[marten#5454](https://github.com/JasperFx/marten/issues/5454).

The Quick append modes have two routes. Most appends go through the dialect-supplied bulk operation,
which already reads back each event's sequence. The other route is the per-event
`QuickAppendEventWithVersion` INSERT, taken when a stream is starting or when an existing stream has
`ExpectedVersionOnServer` set — and on that route `seq_id` comes from an inlined server-side
`nextval()` that **nothing reads back**. The event keeps `Sequence` 0.

That matters because of what consumes `Sequence` afterwards. A unit of work that fails after the
INSERT has already burned the sequence numbers it drew, and the tombstone batch written to fill those
numbers **skips events whose `Sequence` is 0** — it has nothing to write a tombstone against. So the
numbers stay unfilled, and a reader that treats the event sequence as gapless sees a hole that never
closes.

[marten#5456](https://github.com/JasperFx/marten/pull/5456) moved a plain `QuickWithServerTimestamps`
`StartStream` off the per-event route, which is why this is narrower than it sounds — but plain
`Quick`, archived stream partitioning and strict stream identity all still take it, and so does
`FetchForWriting` against a missing stream, because `StartEmptyStream` is a Start.

### The seam

`QuickEventStorageDescriptor` and `QuickWithServerTimestampsEventStorageDescriptor` each gain:

```csharp
public bool AppendEventSqlReturnsSequence { get; init; }
```

Setting it asserts that `AppendEventSqlSuffix` ends in a clause returning `seq_id` — on PostgreSQL,
`returning seq_id`. The storage then wraps the operation so that its `PostprocessAsync` writes the
returned value onto `IEvent.Sequence`, which is what the bulk append path already does.

The wrapping is the whole subtlety. `NoDataReturnedCall` is the marker that tells the unit-of-work
walker **not** to advance the batched reader for an operation, and it sits on
`AppendEventOperationBase`, so a subclass cannot drop it. The flag therefore selects a wrapper that
delegates everything except that marker, rather than a subclass.

## What changes for you

- **Nothing, unless you write an event store dialect on `Weasel.Storage` and set the new flag.** The
  default is `false`, which is the 9.46 behaviour exactly.
- **If you do set it, your `AppendEventSqlSuffix` must return `seq_id`.** The flag is an assertion
  about your SQL, not a request for Weasel to rewrite it. A suffix that returns nothing while the flag
  is set leaves the operation expecting a result row that never arrives.
- **Rich mode is untouched.** The side-effect replay path keeps its old suffix, since its operation
  reads no result.

## New public API

| Member | Default |
| --- | --- |
| `QuickEventStorageDescriptor.AppendEventSqlReturnsSequence` | `false` |
| `QuickWithServerTimestampsEventStorageDescriptor.AppendEventSqlReturnsSequence` | `false` |

## 9.47.1

One PostgreSQL fix. Nothing public was added, removed or changed.

### Indexes are read back in a schema whose name has to be quoted

[#750](https://github.com/JasperFx/weasel/issues/750) /
[#751](https://github.com/JasperFx/weasel/pull/751), contributed by
[@marc-RocketRez](https://github.com/marc-RocketRez), with
[#756](https://github.com/JasperFx/weasel/pull/756) adding coverage.

The index introspection query identified each row's table by `regclassout`'s rendering of it, and
matched that against four spellings of the table Weasel was asking about — bare, schema-qualified,
quoted, and schema-qualified-with-the-table-quoted. **None of the four quoted the schema.**
`regclassout` quotes whatever has to be quoted, so any schema that needs quoting rendered to
something no candidate matched:

| schema | `regclassout` renders | matched |
| --- | --- | --- |
| `public`, in the session's `search_path` | `docs` | yes, the bare candidate |
| `reporting`, not in `search_path` | `reporting.docs` | yes, the qualified candidate |
| `order` | `"order".docs` | **no** |
| `MySchema` | `"MySchema".docs` | **no** |

(All four measured on PostgreSQL 15.3, for a table `docs`.)

So in a schema named for a reserved word — `order`, `user` — or in one carrying uppercase, **no
index was read back at all**, and from there it is
[#504](https://github.com/JasperFx/weasel/issues/504) again. With `actual.Indexes` empty every
declared index is `Missing` rather than `Different`, so the patch is a bare `create index` with no
preceding drop. The first run creates the table and its indexes; every run after it re-issues the
create and PostgreSQL answers `42P07: relation already exists`. A delta executes as one command, so
that first failure aborts everything ordered behind it, the schema never converges, and `db-assert`
can never pass.

Found in Wolverine, which stores its envelope tables in the service's own schema
([wolverine#5215](https://github.com/JasperFx/wolverine/issues/5215)): once
[wolverine#5216](https://github.com/JasperFx/wolverine/pull/5216) quoted the schema in Wolverine's own
SQL, an `order` or a `user` service started once and never again.

The query now returns the table's bare `relname`, from a join on `pg_class` by `idx.indrelid`, and
matches it against `Identifier.Name`. `nspname = :schema` already pinned the query to one schema, so
the name is all that was left to compare, and there is no longer a rendering whose quoting anyone has
to anticipate. That replaces the three extra candidates rather than joining them — they existed only
because `regclassout` quotes, and any spelling it chose that nobody had thought of dropped every index
of the table.

### The primary key constraint's name, in the same schemas

The primary key's name arrives on those same rows, so it was not read back either — and nothing
noticed, because `PrimaryKeyName` falls back to the provider's derived default whenever nothing
assigned it. For a table using the default name the stale actual and the expected agree by
construction, which is why this hid behind the index failure rather than beside it.

Name the constraint yourself and it surfaces. The delta compares the declared name against the derived
default, calls it an `Update`, and emits a rename off a name that was never in the database:

```sql
alter table "order".docs rename constraint pkey_docs_id to docs_pk;
```

```
42704: constraint "pkey_docs_id" for table "docs" does not exist
```

Both are fixed by the one change, and `indexes_in_a_mixed_case_schema` and the two added facts on
`indexes_in_a_reserved_word_schema` hold them.

### What changes for you

- **If any of your schemas is a reserved word or carries uppercase, upgrade.** Nothing else is
  affected: for a schema that needs no quoting, `relname` and `regclassout`'s rendering name the same
  table, and the comparison lands where it always did.
- **Index readback no longer depends on `search_path`.** `regclassout` omitted the schema when the
  schema was in the session's `search_path` and included it otherwise, so which of the four candidates
  matched varied with session state. `relname` is the same string either way.
- **weasel#224 is unaffected.** That is a mixed-case *table* in a lower-case schema, and the
  quoted-name candidates were added for it. It keeps working for a better reason than they were:
  Weasel quotes a mixed-case name when it creates the table, so `relname` is already the
  case-preserved name the configuration asks for.
