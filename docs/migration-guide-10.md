# Migrating from 9.x to 10.0

Weasel 10.0 ships with the **Critter Stack .NET 11 release train**: JasperFx 3.0, Weasel 10.0, Marten 10.0,
Polecat 6.0, Fisher 2.0, Wolverine 7.0 and CritterWatch 2.0 all move to `net10.0;net11.0` together. For Weasel
itself there is no large API rewrite; the work is the target framework floor, newer driver majors, one
newly shipped storage family, and one obsolete method.

## At a glance

| Theme | Effort |
| --- | --- |
| `net9.0` dropped; targets are `net10.0` and `net11.0` | Move your application to `net10.0` or later |
| `JasperFx` / `JasperFx.Events` 3.0 | Pin bump (move the rest of the Critter Stack with it) |
| `Npgsql` 10 on `net10.0` and 11 on `net11.0`, `Microsoft.Data.SqlClient` 7, `Oracle.ManagedDataAccess.Core` 23.26, `MySqlConnector` 3.0 | Pin bump; read the driver release notes |
| `Weasel.EntityFrameworkCore` uses EF Core 10 on `net10.0` and EF Core 11 on `net11.0` | Pin bump. Oracle's EF provider has no EF Core 11 release yet |
| `AdvisoryLock` takes its PostgreSQL advisory locks itself; `Patched.DistributedLock.Postgres` is gone ([weasel#908](https://github.com/JasperFx/weasel/issues/908)) | None; same API and behaviour |
| `Weasel.Sqlite` stays on the `e_sqlite3` bundle, SQLitePCLRaw 3.0.x | None, unless you choose a different bundle |
| `Weasel.Storage` closed-shape document storage family | Additive; only matters if you build a document store on Weasel |
| `MasterTableTenancyBase.TryResolveTenant`: synchronous tenant resolution, no `Task` in the chain ([weasel#838](https://github.com/JasperFx/weasel/issues/838)) | Additive; a store's synchronous tenant lookup can stop blocking on `ResolveTenantAsync` |
| `MasterTableTenancyBase.TryProvisionControlTableSynchronously`: provision the control table without awaiting ([weasel#850](https://github.com/JasperFx/weasel/issues/850)) | Additive; declines by default. A store that overrides it lets `TryResolveTenant` work in a cold process, before any asynchronous call has provisioned the table |
| `ConnectionSourceExtensions.RunSql<T>` is `[Obsolete]` | Switch to `RunSqlAsync<T>` |
| `ISequence.NextLongAsync` / `NextIntAsync` for Hi-Lo ids | Additive; only matters if you build a store on Weasel |
| Sequential `Guid` ids come from JasperFx's `SequentialGuid`, not `Guid.CreateVersion7()` | None for PostgreSQL; a SQL Server store passes `SequentialGuid.SqlServerOrder` |
| MySQL tests for the EF Core bridge are compiled out, and Oracle's on `net11.0` | No library change; Pomelo has no EF Core 10 release, Oracle no EF Core 11 one |
| `net11.0` assemblies are compiled with runtime-async | None; opt out with `-p:WeaselRuntimeAsync=false` when building Weasel from source |

## Target frameworks

Weasel 10.0 drops `net9.0`. Every package targets `net10.0;net11.0`.

```xml
<!-- before (Weasel 9.x) -->
<TargetFrameworks>net9.0;net10.0</TargetFrameworks>

<!-- after (Weasel 10.0) -->
<TargetFrameworks>net10.0;net11.0</TargetFrameworks>
```

An application still on `net9.0` cannot reference Weasel 10.0; stay on the 9.x line until you can move.
Weasel's dependencies follow the target framework: the `net10.0` build stays on the .NET 10 packages, and the
`net11.0` build moves to the .NET 11 ones (Npgsql 11, EF Core 11, `Microsoft.Data.Sqlite` 11,
`Microsoft.Extensions.*` 11). If you pin those packages yourself on `net11.0`, do not pin them below what Weasel
and JasperFx ask for, or restore fails with `NU1605` (package downgrade).

## Package pins

| Package | 9.x | 10.0 |
| --- | --- | --- |
| `JasperFx`, `JasperFx.Events`, `JasperFx.SourceGenerator` | `2.x` | `3.0.0` |
| `Npgsql` | `9.0.x` | `10.0.x` on `net10.0`, `11.0.x` on `net11.0` |
| `Npgsql.NetTopologySuite` | `9.0.x` | `10.0.x` on `net10.0`, `11.0.x` on `net11.0` |
| `Microsoft.Data.SqlClient` | `6.1.x` | `7.1.x` |
| `MySqlConnector` | `2.x` | `3.0.x` |
| `Oracle.ManagedDataAccess.Core` | `23.7.x` | `23.26.x` |
| `Microsoft.Data.Sqlite` | `9.x` | `10.0.x` on `net10.0`, `11.0.x` on `net11.0` |
| `Microsoft.EntityFrameworkCore.Relational` / `.SqlServer` | `9.x` | `10.0.x` on `net10.0`, `11.0.x` on `net11.0` |
| `Npgsql.EntityFrameworkCore.PostgreSQL` | `9.x` | `10.0.x` on `net10.0`, `11.0.x` on `net11.0` |
| `Oracle.EntityFrameworkCore` | not part of the EF Core bridge's suite on `net9.0` | `10.23.x`, on `net10.0` only |
| `Patched.DistributedLock.Postgres` | `1.3.x` | removed |

Weasel's packages are versioned together: bump every `Weasel.*` package your application references in one
change, and do not mix Weasel 10.0 with the Marten, Polecat, Fisher or Wolverine majors of the previous wave.

### Npgsql 10 and 11

`Weasel.Postgresql` is built and tested against Npgsql 10 on `net10.0` and Npgsql 11 on `net11.0`. If your
application pins Npgsql itself, pin the same major for that framework. See the
[Npgsql 10](https://www.npgsql.org/doc/release-notes/10.0.html) and
[Npgsql 11](https://www.npgsql.org/doc/release-notes/11.0.html) release notes for the driver's own breaking
changes. Npgsql 11 removed multiplexing, so a connection string with `Multiplexing=true` fails on `net11.0`.

### AdvisoryLock without DistributedLock

`AdvisoryLock` used to take its locks through `Patched.DistributedLock.Postgres`, which does not work on Npgsql 11
([weasel#908](https://github.com/JasperFx/weasel/issues/908)). It now calls `pg_try_advisory_lock`,
`pg_advisory_unlock` and `pg_try_advisory_xact_lock` itself. The API and the behaviour are the same: the same
lock keys, session locks sharing a connection, transactional locks on a connection each, and with
`LockMonitoringEnabled` on, `HasLock` turns false as soon as the connection holding the lock is lost. The package
dependency is gone, so if your own code used DistributedLock through Weasel, reference it directly.

### Microsoft.Data.SqlClient 7

`Weasel.SqlServer` moves from SqlClient 6 to 7. Weasel's own SQL Server surface is unchanged, but connection
and encryption defaults belong to the driver, so read the SqlClient 7 release notes if your connection strings
rely on a default. Weasel 9.x and the early 10.0 alphas of `Weasel.SqlServer` and `Weasel.Oracle` referenced
`System.Security.Cryptography.Pkcs` directly, at a patched floor (GHSA-846f-f764-76mx). SqlClient 7.1.2 and
`Oracle.ManagedDataAccess.Core` 23.26.401 depend on patched versions themselves, so that reference is gone.

### MySqlConnector 3.0

`Weasel.MySql` moves to MySqlConnector 3.0. Weasel needed no changes for it. Read its
[3.0 release notes](https://github.com/mysql-net/MySqlConnector/releases/tag/3.0.0) for the changes that can
reach your own code, such as `VARBINARY(16)` columns no longer read as `Guid` with `GuidFormat=Binary16`.

### Oracle

`Oracle.ManagedDataAccess.Core` moves to 23.26. With `net9.0` gone, Oracle builds on the same two frameworks as
every other provider and the `Weasel.Oracle` package no longer carries a per-framework reference split.
`Oracle.EntityFrameworkCore` 10 is now supported by the EF Core bridge on `net10.0`, and its tests run in the gate.

### EF Core 10 and 11

`Weasel.EntityFrameworkCore` references EF Core 10 on `net10.0` and EF Core 11 on `net11.0`. An application on
EF Core 9 needs to move to EF Core 10 first.

Oracle has no EF Core 11 provider yet: `Oracle.EntityFrameworkCore` 10.23.x only loads on EF Core 10. An
application that uses Oracle's EF provider with Weasel.EntityFrameworkCore has to stay on `net10.0` until Oracle
ships one. The Oracle half of the EF Core bridge's tests runs on `net10.0` only for the same reason
([weasel#909](https://github.com/JasperFx/weasel/issues/909)).

`Pomelo.EntityFrameworkCore.MySql` has no EF Core 10 release (its latest is `9.0.0-preview.3`), so the
MySQL half of the EF Core bridge's test suite is compiled out. `Weasel.MySql` itself, on `MySqlConnector`, is
unaffected.

## Runtime-async on `net11.0`

The `net11.0` build of every Weasel assembly is compiled with the C# compiler's **runtime-async** feature
(Critter Stack plan decision D4, as in Wolverine 7.0 and JasperFx 3.0). The `net10.0` build is unchanged:
`net10.0` cannot load runtime-async code.

Nothing in your code changes. What you may notice on `net11.0` is how async code looks at runtime: async stack
traces, debugger stepping, and APM agents or profilers that hook compiler-generated async state machines can
behave differently, because those state machines are gone (async iterators keep theirs). Wolverine measured
38–84% fewer allocations on its in-process lane with it on.

If you build Weasel from source and need it off, pass `-p:WeaselRuntimeAsync=false`. The switch does not
trigger a recompile, so clean `bin/` and `obj/` between a build with it and a build without.

## SQLite: the native bundle

`Weasel.Sqlite` references `Microsoft.Data.Sqlite` plus `SQLitePCLRaw.bundle_e_sqlite3` on the SQLitePCLRaw
3.0.x line (currently 3.0.5). `Microsoft.Data.Sqlite` 11 depends on that same bundle and version, so moving to
it needs no change. Weasel does not swap in `SQLite3MC.PCLRaw` (an encrypted build, which is a choice for an
application to make) and does not cap `Microsoft.Data.Sqlite` below 11. An application that wants SQLite3MC can
reference its provider package itself. See [SQLite](/sqlite/) for details.

## `RunSql<T>` is obsolete

`ConnectionSourceExtensions.RunSql<T>(this IConnectionSource<T>, string)` blocks a thread on I/O and is now
marked `[Obsolete]`. It still works in 10.0 and will be removed in a later major version. Use the async
overload:

```csharp
// before
source.RunSql("create table ...");

// after
await source.RunSqlAsync("create table ...");
```

Weasel's own build suppresses obsolete warnings (`NoWarn` includes `0618`); your application does not, so
expect a compiler warning at each call site until you switch.

## `Weasel.Storage`: the closed-shape document storage family

`Weasel.Storage` gains the **closed-shape** document storage family, relocated from Marten so Marten, Polecat
and Fisher share one implementation:

- a `ClosedShapeDocumentStorage` base with four tracking modes (query-only, lightweight, identity map and
  dirty tracking),
- the concrete leaves for flat and hierarchical documents under unversioned, numeric-revision and
  optimistic-version concurrency,
- the insert, update, upsert and overwrite operations, the document metadata binders, and the
  `ClosedShapeDocumentProviderFactory` and `ProviderGraphBase` that build them.

This is additive: nothing already in `Weasel.Storage` changed shape, and an application that only uses
`Weasel.Postgresql`, `Weasel.SqlServer` and the other schema providers does not touch it. It matters to you if
you maintain a document or event store on Weasel, in which case:

- Construct storage through the **non-generic** `ClosedShapeDocumentProviderFactory`. The generic
  instantiations need value-type type arguments, which `MakeGenericType` cannot close under Native AOT.
- A closed-shape identity map refuses an id whose type does not match the document's identity with
  `IdentityMapIdTypeMismatchException`.
- The package is `IsAotCompatible`. As always, publish natively and run the result before believing an AOT
  claim; the analyzer cannot prove the runtime half.

### Pooled `bdata` for per-event appends

The Rich, Quick and QuickWithServerTimestamps event-storage descriptors gain an optional
`WriteEventBdata` slot, `Func<IEvent, IBufferWriter<byte>, bool>`
([weasel#831](https://github.com/JasperFx/weasel/issues/831)). When a store sets it, the per-event operations
Weasel builds (the Rich append, side-effect replay, and tombstones) write each binary event's payload into a
buffer rented from `ArrayPool<byte>.Shared` and bind a slice of it, instead of binding a fresh
`SerializeEventBdata` array per event. Return `false`, writing nothing, for a JSON event. Wire it to
`IEventBinarySerializer.Serialize(Type, object, IBufferWriter<byte>)` from JasperFx 3.3.

Leaving it `null` keeps today's behaviour exactly. A store that sets it must also:

- **Dispose the operations after the whole unit of work**, retries included:
  `if (operation is IDisposable d) d.Dispose()` in a `finally`, as Marten's `SaveChangesAsync` does. That is what
  returns the buffer. Returning it while a retry can still re-execute the batch writes another payload's bytes
  (marten#5262). An operation that is never disposed is only collected, unpooled; a wrapping operation must
  forward `Dispose`.
- **Override `IStorageDialect.SetBinaryParameterValue`** so the slice is bound without a copy. The new member
  defaults to copying it into an array, which every driver accepts. Measured: Npgsql binds a
  `ReadOnlyMemory<byte>` for `bytea` as it is; Microsoft.Data.SqlClient needs a `Stream` over the slice
  (`new MemoryStream(array, offset, count, writable: false)`); Microsoft.Data.Sqlite takes only a `byte[]`, so a
  SQLite store gains nothing from the slot.

### Deriving the closed-shape storages

The tracking-mode bases are public, so a store can derive them to carry its own storage interfaces
(weasel#811): `QueryOnlyClosedShapeDocumentStorage<TDoc, TId>`,
`LightweightClosedShapeDocumentStorage<TDoc, TId>`, `IdentityMapClosedShapeDocumentStorage<TDoc, TId>` and
`DirtyTrackingClosedShapeDocumentStorage<TDoc, TId>`, over the abstract `ClosedShapeDocumentStorage<TDoc, TId>`.
A derived class needs only a constructor: the base builds the write operations and the selector for the
descriptor's concurrency mode. The nine concurrency-mode leaves the factory builds stay internal, and what
the factory builds is unchanged.

Store-specific behaviour stays on `IClosedShapeStorageRules<TDoc, TId>`, which gains defaulted members:

| Member | For |
| --- | --- |
| `BuildUndeleteFragment`, `BuildUndeletion`, `BuildRestoration` | `UndeleteFragment`, `UndeleteForId` and `RestoreForId` on the storage. A restore promises a new version; an undelete does not |
| `TryApplyMappedVersion`, `TryApplyMappedRevision` | The write half of `MappedVersionFor` / `MappedRevisionFor`, also on `IDocumentStorage<T>` with a `false` default |

A hierarchy sub-class is `ClosedShapeSubClassDocumentStorage<TDoc, TRoot, TId>`, which wraps the root's storage
and takes an `IClosedShapeSubClassRules<TDoc, TRoot, TId>` for the discriminator filter, the query filters and
truncation. Build it with `ClosedShapeDocumentProviderFactory.BuildSubClass` or `BuildSubClassStorage`, or
derive it. By default a sub-class truncate empties the **whole hierarchy's table**: answer `TruncateSql` if a
sub-class truncate must leave sibling rows alone.

## Asynchronous Hi-Lo ids

`ISequence` gains `ValueTask<long> NextLongAsync(CancellationToken)` and `ValueTask<int> NextIntAsync(CancellationToken)`
([weasel#808](https://github.com/JasperFx/weasel/issues/808)). On `HiloSequenceBase` they fetch an exhausted
allocation through the existing asynchronous `AdvanceToNextHi`, so a Marten or Polecat sequence gets real
asynchronous I/O with no change to its subclass, and they complete synchronously whenever the id is pure
arithmetic. `NextLong` and `NextLongAsync` can be mixed on one instance without a duplicate id, and a synchronous
`NextLong` never waits on `NextLongAsync`'s asynchronous work
([weasel#833](https://github.com/JasperFx/weasel/issues/833)): a thread blocked on asynchronous work is how a
burst of synchronous callers starves the thread pool.

Both are additive. An `ISequence` that implements neither gets a default that calls its own `NextLong`: a real id,
but obtained synchronously. Three things a `HiloSequenceBase` subclass should know:

- An `AdvanceToNextHi` never overlaps another `AdvanceToNextHi` on one instance, nor an `AdvanceToNextHiSync`
  another `AdvanceToNextHiSync`, but the two **can overlap each other**. A synchronous caller that needs a block
  while an asynchronous advance is in flight reserves one of its own rather than wait. Each must reserve its
  block from the database and install it through `TrySetCurrentHi`. The ids one instance issues still only
  increase: a block that comes back after the other advance installed a higher one is dropped rather than
  installed, and `TrySetCurrentHi` returns `true` for it all the same. The rest of the unused block is a gap in
  the ids, never a duplicate. Outside such an overlap a lower value is installed as given, as before.
- A `SetFloor`, or anything else that installs from outside an advance, can opt into the same rule
  ([weasel#849](https://github.com/JasperFx/weasel/issues/849)). Take the protected `InstallCount` before
  reserving the block and pass it to `TrySetCurrentHi(raw, installCountBefore)`. If another advance installed a
  block since then and this one is not above it, this one is dropped. Without it, an advance that reserved after
  the floor moved and installed first could have its higher block replaced by the floor's lower one, and ids
  would step back (still above the floor and unique). `TrySetCurrentHi(raw)` from outside an advance still
  installs as given.
- An advance must install a block or throw. One that returns having done neither now throws
  `InvalidOperationException`, where it used to issue an id from a block it never reserved.
- `TrySetCurrentHi` with a negative value now returns `false` without touching `CurrentHi`, so one advance's
  failed attempt cannot throw away the block another advance just installed. An override must still not call
  back into `NextLong` or `NextLongAsync`, and `AdvanceToNextHi` should honor its `CancellationToken`.

## Sequential `Guid` ids

`SequentialGuidIdentification<TDoc>`, `ValueTypeIdentification<TDoc, TWrapper, TInner>` and
`ReflectedValueTypeIdentification` no longer call `Guid.CreateVersion7()`. They take a `JasperFx.Core.IGuidGenerator`
through a new constructor overload, and the old constructors use `SequentialGuid.PostgresOrder`.
`Identifications.ForSequentialGuid` and `Identifications.ForValueType` have overloads that take the generator too.

`Guid.CreateVersion7()` is only ordered to the millisecond, and SQL Server does not compare a `uniqueidentifier` in
that order at all. `SequentialGuid` ids always increase within a process, in the order of the database you pick:

```cs
// PostgreSQL, SQLite and anything that stores the id as text: the default
var identification = Identifications.ForSequentialGuid(documentType, idMember);

// SQL Server compares the last six bytes first
var identification = Identifications.ForSequentialGuid(documentType, idMember, SequentialGuid.SqlServerOrder);
```

New ids sort after `Guid.CreateVersion7()` ids written earlier in PostgreSQL order, so an existing table keeps
appending at the end of its primary key index. The `mt_version` values in `Weasel.Storage` come from
`SequentialGuid.NextPostgresOrder()` instead of the obsolete `CombGuidIdGeneration.NewGuid()`.

## Behavior and build changes that need no code

- Several static regular expressions are now source-generated (internal performance work).
- The SQL Server table introspection query reuses one compiled plan.
- Rendered SQL Server update and rollback scripts re-run cleanly, extending what 9.35 did for creation:
  column adds are guarded by `COL_LENGTH`, primary key and check constraint adds by `OBJECT_ID`, and column,
  primary key and table type drops use `IF EXISTS`. `TableColumn.AddColumnSql` now returns two lines, the
  guard and the `alter table`; code that ran it through its own `SqlCommand` keeps working.
- The Nuke build host targets `net10.0`. `./build.sh Pack`, `AotSmoke` and `LocalGate` are the local-train
  entry points; see `CLAUDE.md` in the repository.
- `Weasel.Storage.OperationBatchExecutor.ExecuteAsync` lets only the operation whose statement raised a provider
  error translate it ([weasel#832](https://github.com/JasperFx/weasel/issues/832)). It used to offer the error to
  every operation in order whenever the driver did not name the failing command — on Npgsql, always — so an
  operation matching by error code alone could claim another statement's error. The failing statement is now
  found from where the reader was when the error surfaced, and an error no candidate translates is rethrown as
  itself rather than offered to the rest of the batch. On PostgreSQL, pass
  `OperationBatchOptions.StatementCompletion = NpgsqlStatementCompletion.HasCompleted` to tell apart a run of
  statements that return no rows; without it they are asked in statement order. `ExecuteEachAsync` is unchanged.
- `Weasel.Storage.EventTypeAllowList.For` returns `EventTypeAllowList.All` when any included type is an interface,
  an abstract class or `object` ([weasel#845](https://github.com/JasperFx/weasel/issues/845)). A stored row carries
  its concrete type, so the alias of `IDomainEvent` (from `IncludeType<IDomainEvent>()` or a projection's
  `Apply(IDomainEvent e)`) matched nothing, and the pushed-down filter dropped every event implementing it while the
  shard reported itself caught up. Such a shard now reads every event type and dispatches after hydration, which is
  Marten's long-standing rule.
- `Weasel.Storage.EventTypeAllowList.UpcastSourceAliases` now keeps an upcast source alias that is also a declared
  alias ([weasel#851](https://github.com/JasperFx/weasel/issues/851)). That is the "same event name, new shape"
  upcast: an upcaster from alias `X` to a new type whose alias is also `X`. A dialect matching on `dotnet_type`
  renders `dotnet_type in (DotNetTypeNames) or type in (UpcastSourceAliases)`, and a row written before the
  migration carries the old CLR type name, so with `X` left out it matched neither and the shard skipped it while
  reporting itself caught up. Such a dialect (Polecat's) now reads those rows. `AliasesIncludingUpcastSources` still
  lists each alias once, so a dialect matching on the `type` column (Marten's, Fisher's) renders exactly what it did.
  Code that reads `UpcastSourceAliases` for anything other than that second disjunct should not assume it is
  disjoint from `Aliases`.

- Weasel.Sqlite now depends on Microsoft.Data.Sqlite **10.0.15** ([weasel#857](https://github.com/JasperFx/weasel/issues/857)).
  Up to 10.0.12 its connection pool could lend one native SQLite handle to two open connections when opens of
  one database raced (dotnet/efcore#39008). The symptoms were a transaction that "has completed" under you,
  "unable to delete/modify user-function due to active statements" from `Dispose`, or a process crash in
  SQLitePCLRaw's rollback hook. 10.0.13 fixed it upstream, and NuGet resolves the new floor for any
  application that referenced an older version transitively. That fixes connections an application opens
  itself too, not just Weasel's. SQLitePCLRaw stays on 3.0.5.
- On SQL Server, a schema introspection read chosen as a deadlock victim (error 1205) is read again instead of
  failing the migration ([weasel#900](https://github.com/JasperFx/weasel/issues/900)). It happened when another
  connection ran DDL against the same database at the same time. A 1205 while applying DDL is still not retried.
- `Weasel.Storage.EventLoaderBase` honors `EventRequest.ParksUnreadableEvents`
  ([weasel#901](https://github.com/JasperFx/weasel/issues/901)). With it set, an event whose body will not
  deserialize is reported on `EventPage.Unreadable` and counted as skipped, rather than thrown or passed to
  `RecordSkippedEventAsync`. Without it, nothing changes.

## Dependency lockstep

| Weasel | JasperFx | JasperFx.Events | Marten | Polecat | Fisher | Wolverine |
| --- | --- | --- | --- | --- | --- | --- |
| 10.0 | 3.0 | 3.0 | 10.0 | 6.0 | 2.0 | 7.0 |

Mixing majors across products is unsupported in this wave.

## References

- [Migrating from 8.x to 9.0](/migration-guide)
- [Provider Trait Matrix](/core/provider-trait-matrix)
- [Weasel 10.0 migration guide tracking issue](https://github.com/JasperFx/weasel/issues/746)
