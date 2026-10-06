# Upgrading to 9.42

Three unrelated things: a SQL Server advisory lock that stopped reporting locks it had lost, the
other half of 9.41's Native AOT work, and table storage parameters on PostgreSQL.

**Nothing in this release is a breaking change.** Nothing public was removed and no signature
changed. One behaviour *does* change, deliberately, and it is the fix rather than a side effect —
see [the advisory lock section](#sql-server-advisory-locks-end-with-their-session), which is the one
part of this release worth reading before you upgrade.

## SQL Server advisory locks end with their session

[#695](https://github.com/JasperFx/weasel/pull/695), from
[@NielsAudoor](https://github.com/NielsAudoor). **If you run Polecat HotCold, or anything else built
on `Weasel.SqlServer.AdvisoryLock`, upgrade.**

`AdvisoryLock` holds every lock as a session-scoped `sp_getapplock` on one connection. When SQL
Server ends that session — failover, gateway reconfiguration, a killed session — it releases all of
them. `HasLock` went on returning `true` anyway:

- a node holding every lock it wants never uses the connection again, so it never learns the session
  is gone;
- a node that reconnects to poll for a *different* lock kept its old list of held locks.

Another node then takes the lock and both run the shard. In a three-replica load test, 22,000 outbox
events were processed more than once.

The root cause is one character of pattern matching, which is worth recording because it reads as
correct:

```csharp
// Before. A null _conn does not match the pattern, so `is not` SUCCEEDS --
// HasLock returned true for a lock whose connection no longer existed.
return _conn is not { State: ConnectionState.Closed } && _locks.Contains(lockId);

// After.
return _conn is { State: not ConnectionState.Closed } && _locks.Contains(lockId);
```

### What changes for you

- **Locks are dropped together with their session,** and `HasLock` now needs a live connection. A
  node that has lost its session reports `false`, so `ProjectionCoordinatorBase` re-attains on its
  next cycle, fails because the other node holds it, and **stops the agents on this node.** That is
  the split brain closing, not merely being detected.
- **A monitor checks `APPLOCK_MODE` every 5 seconds while locks are held** and drops any the session
  no longer holds. This is the SQL Server counterpart of the Postgres lock's
  `LockMonitoringEnabled`, which Marten turns on by default through `UseMonitoredAdvisoryLock`. It is
  **always on** here and is not yet configurable: Polecat constructs the lock without options, and
  leaving monitoring off is the split brain. The probe short-circuits when no locks are held, so an
  idle lock costs nothing.
- **`TryAttainLockAsync` no longer waits for a lock another node holds.** It passes
  `lockTimeoutMs: 0` instead of the 1000 ms default. The coordinator loops over every projection set
  in sequence, so a blocking wait per contended lock serialized into real latency across the whole
  distribution. This is a behaviour change for *every* caller of the method, not only Polecat's
  coordinator.
- **A lock you already hold is no longer taken twice.** `sp_getapplock` is reentrant, so a second
  acquire needed two releases to free it — and `_locks` was a `List<int>`, so the duplicate entry
  meant `ReleaseLockAsync` removed one and released once, leaving the lock held on the server while
  Weasel believed it was free. `TryAttainLockAsync` now returns `true` for a held lock without
  touching the server.

A `SemaphoreSlim` serialises access to the connection, since the monitor shares it. That also fixes
concurrent acquires racing on one `SqlConnection`, which never supported concurrent commands.

::: warning Monitoring is not configurable yet
One `AdvisoryLock` is created per database, so a deployment with many databases gets one polling
timer per database whose shards this node owns. If that matters for your topology, say so on
[#695](https://github.com/JasperFx/weasel/pull/695) — an options object matching
`Weasel.Postgresql.AdvisoryLockOptions` is the obvious next step.
:::

## The rest of Native AOT identity

[#697](https://github.com/JasperFx/weasel/pull/697), from
[#694](https://github.com/JasperFx/weasel/issues/694). Finishes what
[9.41](/release-9-41) started.

9.41 added `Identifications`, a non-generic factory so a `Type`-keyed consumer never closes one of
Weasel's identity generics itself, and gave `ForValueType` a reflected fallback for the strong-typed
id case. Five of the other six factories still threw under Native AOT:

```
System.NotSupportedException: 'Weasel.Core.Identity.SequentialGuidIdentification`1[Doc]'
is missing native code or metadata.
```

So the factory worked for the hard case and failed for all the easy ones.

### Why the easy case was the broken one

9.41's reasoning was that these six close a generic over the document type alone, a document type is
a class, and reference-type instantiations share one canonical body — so `MakeGenericType` is fine.
That is half the rule. **They share a canonical body only if ILC generated one,** and nothing
statically references `SequentialGuidIdentification<anything>`, so there was nothing to share.

`[DynamicDependency(DynamicallyAccessedMemberTypes.PublicConstructors, typeof(Strategy<>))]` on each
factory roots both the canonical body and the constructor metadata. `DynamicallyAccessedMembers` on
an **open** generic does carry to the closed instantiation — the part worth measuring rather than
assuming.

::: tip The exception you get depends on your assembly
[#694](https://github.com/JasperFx/weasel/issues/694) was reported as a `MissingMethodException`
from `Activator.CreateInstance`, not the `NotSupportedException` above. It is the same missing root
one layer further in: a consumer that constructs the same strategy itself roots the canonical body
*by accident*, leaving only the trimmed constructor metadata to fail on. Which one you hit depends
on what else your assembly happens to reference — so there was never a safe subset here.
:::

Nothing about this is reachable by the IL analyzer, because whether a canonical instantiation exists
is a property of the native image rather than of the IL. `Weasel.Core.AotSmoke` therefore constructs
every factory **and uses it** — the constructors build FEC-compiled accessor delegates, so
constructing successfully is not the same as working — and the check is a native publish-and-run.

**No API changed.** If you are not publishing natively, nothing here affects you.

## PostgreSQL table storage parameters

[#696](https://github.com/JasperFx/weasel/pull/696), from [@lahma](https://github.com/lahma), with
[#698](https://github.com/JasperFx/weasel/pull/698) adding the named API over it.

A table can now declare PostgreSQL storage parameters — `fillfactor`, the `autovacuum_*` family and
the rest of the `reloptions` — and Weasel keeps them in step like any other part of the table.
Previously `CREATE TABLE` wrote no `WITH (...)`, `FetchExisting` never read `pg_class.reloptions`,
and `TableDelta` could not see them, so the only option was a hand-applied `ALTER TABLE` that Weasel
could neither report as drift nor restore when it recreated the table. Index storage parameters have
been supported since #37; this is the table half.

```csharp
var table = new Table("public.events");
table.AddColumn<Guid>("id").AsPrimaryKey();

table.WithFillFactor(70)
    .WithAutovacuum(vacuumScaleFactor: 0.01, insertScaleFactor: 0.02)
    .WithParallelWorkers(4)
    .WithStorageParameter(StorageParameterNames.VacuumTruncate, false);
```

The dictionary form works too, and `StorageParameterNames` is not only convenience there:

```csharp
table.StorageParameters[StorageParameterNames.FillFactor] = 70;
```

`StorageParameters` keys are case **sensitive**, while the DDL writer and the catalog reader both
normalize to lower case — so `"FILLFACTOR"` and `"fillfactor"` are two entries that render as one
duplicated setting, which PostgreSQL rejects with 22023. Weasel now refuses that itself with a
message saying why, and the constants make it unreachable.

### What the delta does, and does not, touch

- **Only the parameters the table declares are compared.** A parameter that exists in the database
  but is not declared on the table is **never reset**, because a DBA may have set it deliberately. A
  table that declares none behaves exactly as before: no extra SQL, no delta.
- The same is true of the fluent methods' optional arguments. `WithAutovacuum` declares only what you
  pass, so leaving one out means "say nothing about this one" rather than "reset it".
- Values compare case-insensitively, and **numerically when both sides are numbers**. PostgreSQL
  stores the text as written, so `0.05` and `0.050` are the same setting and do not produce a
  permanent false diff.
- A difference is an in-place `Update` written as `ALTER TABLE ... SET (...)`. The rollback restores
  the previous value, or `RESET`s a parameter that was not set before.
- For a **partitioned** table PostgreSQL rejects storage parameters on the parent, so Weasel writes
  them on every partition it creates — declared list, range and hash partitions, the default
  partition, and the ones `ManagedRangePartitions` adds later — and applies `ALTER TABLE` to each
  existing partition when they change. Only the **direct** partitions are inspected, so a
  sub-partitioned tree is not recursed into.
- `toast.*` parameters are refused. They are real parameters, but they live on the TOAST relation's
  own `reloptions` rather than the table's, so Weasel could write one and would then read back
  nothing and report a difference forever.

See [PostgreSQL tables](/postgresql/tables#storage-parameters) for the full documentation.

## New public API

- `Weasel.Postgresql.Tables.Table.StorageParameters` and `Table.FillFactor` — the declared storage
  parameters, shaped like `IndexDefinition.StorageParameters`.
- `Weasel.Postgresql.Tables.StorageParameterNames` — the table-level reloption names as constants.
  `toast.*` names are deliberately absent, since Weasel refuses them.
- `Table.WithStorageParameter`, `WithFillFactor`, `WithAutovacuum`, `WithParallelWorkers` and
  `WithAutovacuumLogging` — the fluent form, each routed through those constants.
- `PartitionExtensions.WriteDefaultPartition(TextWriter, Table)` — a new overload that carries the
  table's storage parameters onto the default partition. The existing `DbObjectName` overload is
  unchanged.
