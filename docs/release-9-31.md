# Upgrading to 9.31

9.31.0 is the 2026-09-05/06 wave: a run of internals lifted out of Marten, Polecat and Fisher into
Weasel so the three stores stop carrying three copies of the same code, plus a performance pass over
the read and parameter paths. 9.31.1 adds one fix on top of it.

::: tip Coming from 9.30?
Nothing in the 9.31 line changes what a migration does to an existing database. The lifts are
additive — new types in `Weasel.Storage` and `Weasel.Core`, with the stores adopting them
separately — and the two behaviour changes worth knowing about are both called out below.
:::

## What 9.31.1 fixes

### A shared partition manager could be reloaded out from under a concurrent reader

[#584](https://github.com/JasperFx/weasel/pull/584), closing
[#583](https://github.com/JasperFx/weasel/issues/583), raised from
[JasperFx/marten#5364](https://github.com/JasperFx/marten/issues/5364).

`ManagedListPartitions` kept its tenant registry in a plain `Dictionary` that it mutated **in
place**, and published it two ways that both hand out the live instance: `Partitions` returns a
`ReadOnlyDictionary`, which wraps rather than copies, and `IListPartitionManager.Partitions()` is a
lazy iterator over the same dictionary — the one `ListPartitioning.Partitions()` calls for DDL and
delta calculation. `InitializeAsync` then `Clear()`ed and refilled that instance. The semaphore in
there serializes writers against each other and does nothing for readers.

One manager instance is shared across every database in a store, so `db-apply --parallel N` over
more than N databases has a late-starting worker reloading the registry while an earlier worker is
still enumerating it. The reported symptom was

```
System.InvalidOperationException: Collection was modified; enumeration operation may not execute.
```

out of `DatabaseBase.assertValidIdentifiers`, on 1 of 29 databases at `--parallel 16`, with an
immediate retry clean.

**The quieter failure is the one worth upgrading for**, because nothing reports it: `Clear()`-then-
refill also lets a reader observe an *empty or partial* partition set and generate a script, or
compute a partition delta, against it. Script-generation paths are the obvious exposure since they
do not hold the migration's global lock.

Hardening the read side does not fix this, which is worth recording because it is the tempting
change. `ToArray()` over the published map routes through `ICollection<T>.CopyTo`, which has no
version check and so cannot throw `InvalidOperationException` — it reads `Count`, the dictionary
grows underneath, and it throws `Destination array is not long enough` instead. Any read-side
snapshot against an in-place-mutated dictionary only changes which exception you get, or tears
silently.

Both managers now publish an immutable snapshot and swap it copy-on-write: build or copy a detached
map, mutate that, assign it in one write. Readers capture the field once and enumerate a complete,
consistent map. A dedicated lock serializes the read-copy-write sequences — deliberately not the
existing semaphore, which is held across database round trips inside `InitializeAsync`.

`Weasel.SqlServer`'s `ManagedTenantPartitions` had the identical shape and gets the same treatment,
which matters for Polecat: `_ordinals` and `_buckets` were published as live wrappers, cleared and
refilled in the initializer and the reset path, and enumerated by `OrderedBoundaries()` straight off
the schema-object path that emits partition-function DDL. The two maps swap **together**, because
the drop path prunes buckets against the ordinals it just removed — a reader that saw one updated
and not the other would resolve a bucket to a released partition.

This is latent, not a recent regression. Two smaller corrections come with it:

- `ManagedListPartitions.ResetValues` copies the dictionary it is given instead of aliasing it, so a
  caller that goes on to mutate its own dictionary is no longer mutating the manager's state.
- The batch `AddPartitionToAllTables` records the new values after its transaction commits rather
  than mid-loop, so a failed insert no longer leaves partially-registered tenants in memory.

## What 9.31.0 changed

### A never-created stream is at version 0, not a conflict

[#580](https://github.com/JasperFx/weasel/pull/580), for
[JasperFx/marten#5345](https://github.com/JasperFx/marten/issues/5345).

`AssertStreamVersionOperation.PostprocessAsync` treated "no row came back" as an unconditional
conflict, so `AlwaysEnforceConsistency` on a never-created stream threw `Unexpected starting version
number ... expected 0 but was 0` — the guard firing where expectation and reality agree. A stream
with no row is at version 0; that is a conflict only when the caller expected otherwise. The
operation is shared with Polecat, which is why the fix lives here rather than in Marten.

### JSON column reads stream instead of materializing a UTF-16 string

[#576](https://github.com/JasperFx/weasel/pull/576), closing
[#573](https://github.com/JasperFx/weasel/issues/573).

`SystemTextJsonSerializer`'s four `DbDataReader` overloads read every JSON column through
`GetString` / `GetFieldValueAsync<string>`, and System.Text.Json then transcoded that fresh UTF-16
string straight back to UTF-8 to parse it. They now take the column as a `Stream` where the provider
supports it and fall back to the string path where it does not — Microsoft.Data.SqlClient refuses
`GetStream` on `nvarchar(max)` and on SQL Server 2025's native `json`, and sends both as UTF-16 on
the wire anyway, so the string path is already the right one there.

Measured on SQLite: **20% fewer allocated bytes at 5 KB, 21% at 120 KB, and Gen2 collections halved
at 120 KB** — a 120 K-character document is 240 KB of UTF-16 and was landing on the large object
heap on every read. This is an allocation and GC-pressure win, **not** a throughput win; wall clock
stayed inside measurement noise.

The capability is learned once per reader type by attempting the call and remembering a refusal,
rather than enumerating providers — which would be wrong for the next one.

### ⚠️ The ChangeTracker `DeepEquals` fallback is opt-in now

[#578](https://github.com/JasperFx/weasel/pull/578), closing
[#577](https://github.com/JasperFx/weasel/issues/577).

`ChangeTracker.DetectChanges` runs once per tracked document on every `SaveChanges`. When the
ordinal JSON compare missed, it fell into two `JsonNode.Parse` calls and a `JsonNode.DeepEquals` —
two object graphs built and discarded to reach an answer the string compare had already implied.

That fallback now sits behind `IStorageSession.UseSemanticJsonChangeDetection`, a default interface
member defaulting to `false`. No existing implementor has to change, **but the semantic compare is
off unless you turn it on.** It was gated rather than deleted for a real reason: `Dictionary<TKey,
TValue>` and `HashSet<T>` members serialize in enumeration order, which follows insertion and
removal history rather than contents, so a rebuilt dictionary serializes to different text for the
same document under one deterministic serializer. If your documents contain dictionaries or sets
that get rebuilt, set the flag.

### ⚠️ A member-valued flat-table decrement inserts the negated parameter

[#574](https://github.com/JasperFx/weasel/pull/574), applying the ruling on
[JasperFx/jasperfx#773](https://github.com/JasperFx/jasperfx/issues/773).

For a member-valued `Decrement(x => x.Amount)` landing on a row that does not exist yet, the insert
branch applies the event to an implicit zero row: a first sighting of `5` lands the column at
**-5**. A decrement must never raise a column. Marten already did this; the lifted maps took the
other reading and are the ones that change. Only the **member-valued** form was ruled on.

### Internals lifted out of the three stores

A run of relocations, each store-neutral and additive. Nothing in Marten, Polecat or Fisher changes
until those stores adopt them, which is a separate gated step.

- [#559](https://github.com/JasperFx/weasel/pull/559) — the System.Text.Json serializer trio shared
  by Polecat and Fisher becomes `Weasel.Core.SystemTextJsonSerializer` and friends.
- [#563](https://github.com/JasperFx/weasel/pull/563) — a generic
  `Weasel.Storage.ITransactionParticipant<TConnection, TTransaction>`, the shape all three stores
  declare closed over their own provider types.
- [#569](https://github.com/JasperFx/weasel/pull/569) — the flat-table projection DSL, implemented
  three times with the same public shape.
- [#570](https://github.com/JasperFx/weasel/pull/570) — a dialect-neutral `EventLoaderBase`; the
  async daemon's inner event loader is the same shape in all three stores, differing only in the
  batch-limit syntax.
- [#571](https://github.com/JasperFx/weasel/pull/571) — `MasterTableTenancy` into
  `Weasel.Core.MultiTenancy`, over a `DbDataSource` seam.

### Performance

- [#558](https://github.com/JasperFx/weasel/pull/558) — positional parameter names are precomputed
  instead of allocating `"p" + count` per parameter.
- [#560](https://github.com/JasperFx/weasel/pull/560) — introspection parameter costs are recorded
  from the real render rather than a probe pass.
- [#564](https://github.com/JasperFx/weasel/pull/564) — SQLite PRAGMAs are classified by their
  actual scope. `page_size`, `auto_vacuum`, and `journal_mode` when WAL apply **once per data
  source** rather than per connection, because they take effect before the file is first written or
  are persisted in the database file header. The batches are precomputed.
- [#572](https://github.com/JasperFx/weasel/pull/572) — `Weasel.Benchmarks`, carrying the baselines
  behind the numbers quoted above.
