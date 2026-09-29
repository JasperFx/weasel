# Upgrading to 9.37

9.37 is five fixes, and one of them is destroying data right now for anyone on SQL Server 2025 with
a JSON index. The rest are a drift fix on Oracle and three defects on the migration path itself: a
delta that threw instead of answering, a global lock that a failed migration never gave back, and a
connection the reconnection loop walked away from.

::: warning On SQL Server 2025 with a JSON index? Upgrade before your next migration.
A JSON index was read back as an index with **no columns**, matched nothing your model declared,
and was emitted as a `DROP INDEX`. Every apply silently removed it. See below.
:::

::: tip Coming from 9.36?
Nothing you had settles differently, with one deliberate exception: a migration that used to strand
the global migration lock now releases it, so replicas that were being locked out will start
migrating again. If you have monitoring that keys on the *symptom* — "Unable to attain the global
lock in time to apply database changes" — expect it to go quiet.
:::

## ⚠️ A SQL Server JSON index was migrated away

[#661](https://github.com/JasperFx/weasel/pull/661).

A JSON index is in `sys.indexes` with `type_desc = 'JSON'`, but its `sys.index_columns` row carries
`key_ordinal = 0` — and the column query requires `key_ordinal >= 1 or is_included_column = 1`. So
the index came back with no columns, matched nothing declared, landed in `Indexes.Extras`, and
`TableDelta` emitted a `DROP INDEX` for it.

**Any consumer on SQL Server 2025 with a JSON index has a migration that silently removes it.** The
ordinary index query now excludes `type_desc = 'JSON'`, and JSON indexes are read through
`sys.json_indexes` and `sys.json_index_paths` instead.

### Declaring one

The same fix adds `JsonIndexDefinition`, so SQL Server 2025's `CREATE JSON INDEX` can be declared,
rendered, compared and asserted on like any other schema object:

```csharp
var table = new Table("documents");
table.AddColumn<int>("id").AsPrimaryKey();
table.AddColumn("data", "json");

var index = new JsonIndexDefinition("idx_documents_data", "data");
index.Paths.Add("$.customer.id");
index.OptimizeForArraySearch = true;
table.Indexes.Add(index);
```

It is an `IndexDefinition` subclass rather than a parallel collection, so it flows through
everything a table already does with its indexes. Options a JSON index has no grammar for —
`UNIQUE`, `CLUSTERED`, `INCLUDE`, a filter predicate, sort order — are **refused rather than
ignored**, because dropping one silently would build a different index than the model describes
while the model kept reporting a match.

`sys.json_indexes` does not exist before SQL Server 2025, and a batch that merely names it fails to
*compile* there — which would break reading every table, not just one with a JSON index. Both new
result sets go through `sp_executesql` behind an `object_id` check, so **nothing changes for you on
2022 or earlier.**

## Oracle: a descending index never settled on the migration path

[#660](https://github.com/JasperFx/weasel/pull/660), contributed by
[@lahma](https://github.com/lahma), found through Quartz.NET's Weasel integration.

`PRIORITY DESC` read back as its hidden `SYS_NC00007$` column, so the index never matched its model
and every apply rebuilt it. Two views migrated together were replaced on every apply for the same
reason.

The cause is worth knowing if you write introspection queries for Oracle. `CompileCommands` creates
a new `OracleCommand` per statement when a batch holds several — ODP.NET cannot execute several
statements from one command — and those split commands dropped `InitialLONGFetchSize`. Without it
ODP.NET reads a LONG column back **empty**, and both `ALL_IND_EXPRESSIONS.COLUMN_EXPRESSION` and
`ALL_VIEWS.TEXT` are LONGs.

It therefore failed only in combination: the object was correct on its own, and wrong as soon as it
was migrated alongside anything else. Split commands now carry `CommandTimeout`, `FetchSize`,
`InitialLONGFetchSize` and `InitialLOBFetchSize` from the command being built.

## A delta for a table that does not exist answers instead of throwing

[#658](https://github.com/JasperFx/weasel/issues/658).

`TableDelta.compare()` returns early when there is no existing table, so the `ItemDelta` fields it
otherwise assigns stayed null and `HasChanges()` threw:

```
System.NullReferenceException: Object reference not set to an instance of an object.
   at Weasel.SqlServer.Tables.TableDelta.HasChanges()
```

A missing table is the one case whose answer is unambiguously *yes, there are changes* — it needs
creating. Fixed on SQL Server, PostgreSQL and SQLite; Oracle and MySQL were already safe.

This one reached operators through Wolverine, which calls `FindDeltaAsync(...).HasChanges()` on its
queue tables during a resource check, before those tables are provisioned. `BrokerResource.Check`
catches any exception and reports the endpoint as *missing*, so the real cause appeared only as a
separate error log line above `Missing known broker resources: sqlserver://...`.

## A failed migration no longer strands the global lock

[#659](https://github.com/JasperFx/weasel/issues/659).

`ApplyAllConfiguredChangesToDatabaseAsync` released the global migration lock only on its success
paths. An exception anywhere between attaining the lock and finishing — `initializeSchema`,
`SchemaMigration.DetermineAsync`, `Migrator.ApplyAllAsync` — skipped the release, and the `finally`
only disposed the connection.

Every other store or replica sharing that database was then locked out of migrating, reporting
`Unable to attain the global lock in time to apply database changes` — whose own advice,
`ResourceMigrationFailureMode.ContinueOnFailures`, would start them against an unmigrated schema.
The node that stranded the lock reported nothing at all, which is why this went unnoticed.

### Why disposing the connection was not enough

It looks as though it should be. The lock is session-scoped, and Npgsql resets a pooled connection
with `DISCARD ALL`, which does release session advisory locks. But **the reset happens when the
connection is next used, not when it is returned** — so the lock outlives the apply for as long as
that connection sits idle in the pool. In a Marten test run one connection was observed holding the
lock in `state=idle` for the rest of the run.

The release now runs on the way out as well, guarded on having actually attained the lock. It passes
`CancellationToken.None`, because a cancelled apply is one of the ways to get there and the caller's
token would mean never releasing in exactly that case. A release that itself fails is swallowed: the
migration failure is the one you need to see.

## The reconnection loop no longer abandons a connection

[#664](https://github.com/JasperFx/weasel/issues/664).

When `TryAttainLock` reports that it should reconnect — a terminated backend, an unreachable server
— the loop replaced its connection without disposing the one it overwrote, and the `finally` only
ever sees the last one. Every reconnection attempt left a live connection for the finalizer to find,
holding its socket and its slot in the driver's pool, against a server that had just refused or
terminated a connection. That is what put the loop there in the first place.

## New public API

- `Weasel.SqlServer.Tables.JsonIndexDefinition` — SQL Server 2025 `CREATE JSON INDEX`, with `Paths`,
  `FillFactor` and `OptimizeForArraySearch`.
- `ItemDelta<T>.AllMissing(...)` on SQLite — the delta against no actuals at all. Deliberately does
  no name pairing, because the pairing constructor refuses a table declaring two names that differ
  only in case, and a table being created for the first time is still allowed to do that.
