# Upgrading to 9.27

9.27 is a bug-fix release. Nine changes, and every one of them is a case where Weasel read a schema
back as something other than what the database actually held. Nothing here changes emitted DDL for
a schema Weasel was already reading correctly.

::: warning Three of these were permanent
A named foreign key on SQLite, a partition-aligned index on SQL Server, and a concurrent index on a
manager-owned partitioned table each produced a migration that **could never converge**. The
generated patch ran, the next run produced the same patch, and it repeated forever. If any of those
describes your schema, the repetition stops when you upgrade.
:::

## The ones that never converged

A delta that cannot converge is worse than a wrong one. The patch is applied, the read-back is
still different, and the next migration run produces the identical patch — indefinitely.

### A named foreign key on SQLite rebuilt the table on every run

[#516](https://github.com/JasperFx/weasel/pull/516). `pragma_foreign_key_list` has no name column at
all, so the introspection invented one: `fk_{table}_{reftable}_{id}`. Every foreign key Weasel had
itself written therefore read back under a name the model did not have — the delta saw one
constraint missing and one extra.

On SQLite that is repaired by **rebuilding the table and copying every row**. The rebuilt table
reads back exactly the same way, so it happened on every run.

The name now comes from the stored `CREATE TABLE` text. An inline `REFERENCES` has no name to read,
so the synthesised one still stands for those.

### A partition-aligned index on SQL Server was dropped and recreated on every run

[#512](https://github.com/JasperFx/weasel/pull/512). An index aligned with its table's partition
scheme carries an implicit row in `sys.index_columns` for the partitioning column — `key_ordinal 0`,
`is_included_column 0`, `partition_ordinal >= 1`. The index-column query had no predicate, so that
row was read as a declared key column and the index compared unequal to itself.

Rebuilding it produced another aligned index, which read back the same way.

### A concurrent index on a manager-owned partitioned table stayed invalid

[#520](https://github.com/JasperFx/weasel/issues/520). `ListPartitioning` resolved its partitions
manager-first when writing DDL and when computing deltas, but not when enumerating partition table
names. Since `UsePartitionManager` also clears `EnableDefaultPartition`, a manager-owned
partitioning enumerated the **empty** sequence.

The three-step concurrent index sequence is built from those names, so only the first step
rendered:

```sql
CREATE INDEX idx_mt_events_tags ON ONLY schema.mt_events USING gin (tags);
```

That is metadata-only. The parent index is invalid by design until every child index is attached,
and no child ever was — so the planner would not use it, and
[#508](https://github.com/JasperFx/weasel/issues/508) correctly reported it as drift on every
subsequent apply. Strictly worse than the blocking `CREATE INDEX` it replaced, and `IsConcurrent`
is set precisely by someone trying to avoid an outage.

## A migration that failed outright

### A procedure, user-defined type or trigger broke any migration it shared

[#515](https://github.com/JasperFx/weasel/pull/515),
[#518](https://github.com/JasperFx/weasel/issues/518). `SchemaMigration` concatenates every object's
introspection query into a single command, so each query has to terminate itself. Several did not,
and the statement that followed ran into the previous one:

```
Npgsql.PostgresException : 42601: syntax error at or near "select"
```

Any migration containing one of them **plus another object** failed, unless the unterminated one
happened to be last. That ordering is why it went unnoticed: each type had tests, and each was
tested alone.

Fixed on PostgreSQL (stored procedure, user-defined type, trigger), SQLite (trigger) and MySQL
(function, stored procedure, trigger, view). SQL Server's six were unaffected in practice because
T-SQL treats the statement separator as optional, but they are terminated now too.

Oracle is deliberately excluded: ODP.NET executes one statement per command, so its builder splits
the batch and a terminator there would be a syntax error rather than a fix. A conformance test now
asserts all three facts, and fails when a new schema object type is added without being covered.

## Composite keys and index order

### A composite foreign key could pair the wrong columns

[#511](https://github.com/JasperFx/weasel/pull/511) (SQL Server),
[#516](https://github.com/JasperFx/weasel/pull/516) (SQLite). Both sides of a foreign key were
sorted independently, which kept each list tidy and destroyed the pairing between them. A key
declared `(x, y) REFERENCES parent (b, a)` was read back as `(x, y) REFERENCES parent (a, b)` — a
constraint over different columns entirely.

Declaration order is now preserved on both sides, and equality compares the *pairs*, so the same key
written in another order still compares equal.

### Composite primary key order was discarded

[#511](https://github.com/JasperFx/weasel/pull/511),
[#516](https://github.com/JasperFx/weasel/pull/516),
[#517](https://github.com/JasperFx/weasel/issues/517). Column order is part of a composite key's
identity: `(a, b)` and `(b, a)` are different indexes with different query plans. SQL Server read
the key through a catalog view that cannot express order, SQLite read `pragma_table_xinfo.pk` as a
flag when it is a 1-based position, and Oracle and MySQL read the order correctly and then
discarded it.

All five providers now read it faithfully. **Reading it did not become comparing it** — see below.

### An index's key direction is read per column

[#513](https://github.com/JasperFx/weasel/pull/513). SQL Server sets sort direction per column.
Weasel could only say "this index is descending", which appends a single trailing `DESC` and
therefore marks the **last** key column. An index created as `(a DESC, b)` was read back as
`(a, b DESC)`, and a model declaring `(a, b DESC)` compared *equal* to it — so a genuinely different
index was never reported as drift.

Direction is now read into `DescendingColumns` and rendered per column. Comparing it is opt-in
through `CompareColumnDirection`.

### Hash partitions compared by catalog order

[#514](https://github.com/JasperFx/weasel/pull/514). `CreateDelta` sorted both sides by modulus
before comparing, which sorts nothing on a table whose partitions all share one — and the actual
side comes from `pg_inherits`, which has no inherent order. A hash-partitioned table compared equal
or unequal *to itself* depending on what the catalog happened to return, and could report
`PartitionDelta.Rebuild` on a table that had not changed.

## Column fidelity on SQL Server

[#511](https://github.com/JasperFx/weasel/pull/511). `information_schema.columns` reports length in
characters and has no `is_identity`, so `decimal(18,2)` read back as bare `decimal` and IDENTITY was
never read at all. Anything regenerating DDL from a fetched table — rollback above all — emitted a
narrower column than the one in the database, and `(18,0)` truncated the scale. Columns now come
from `sys.columns`.

`IgnoreIndex` also did nothing on SQL Server, though PostgreSQL and SQLite honour it: the generated
patch dropped the very index it was meant to protect.

## New API

`SetPrimaryKeyOrder` and `HasExplicitPrimaryKeyOrder` are on `TableBase`, so every provider has
them ([#517](https://github.com/JasperFx/weasel/issues/517)).

```csharp
var table = new Table("orders");
table.AddColumn<int>("tenant_id").AsPrimaryKey();
table.AddColumn<int>("id").AsPrimaryKey();

// The key is (id, tenant_id), whatever order the columns were flagged in
table.SetPrimaryKeyOrder(["id", "tenant_id"]);
```

The list must name every column of the key, exactly once, and nothing outside it — a partial pin
would opt the table into strict order comparison while silently reordering the columns it did not
name, so it is rejected.

### Why pinning is opt-in

Reading key order faithfully must not turn into *comparing* it for a model that never expressed one.
A model that only flags columns with `AsPrimaryKey()` cannot say `(c, a)`, so comparing order
unconditionally would report drift on every such table — drift the user cannot resolve, "fixed" by
rewriting their key. That is a table rebuild on a clustered key, and a full row copy on SQLite.

So order is compared only when it was pinned. PostgreSQL is the exception and always compares it:
it stores the key as an explicit list and so can express order natively.

`IndexDefinition.DescendingColumns` and `CompareColumnDirection` are new on SQL Server, opt-in for
the same reason.

## Upgrading

Nothing to do. No configuration changes, and no DDL changes for a schema Weasel was already reading
correctly.

If one of the non-converging cases above describes your schema, the first run after upgrading
applies the last copy of the repeating patch and then stops producing it.
