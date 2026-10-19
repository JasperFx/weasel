# Upgrading to 9.46

Four fixes. Three of them are cases where Weasel reported or did the wrong thing while every check said
the schema was fine, and the fourth is a trim warning that reached anyone publishing trimmed or
natively compiled.

- A concurrent index on a partitioned table that stopped partway through could never be finished.
- `AdvisoryLock.FindHolderAsync` could name a holder that lives in a different database.
- A Firebird migration could fail reading a catalog another applier was rewriting.
- `Weasel.Core` produced a trim warning for any application that keeps `MultiCommandDataReader`.

**Nothing public was removed and no signature changed.** One member was added to `Migrator`, with a
default implementation, so existing subclasses keep working. Read
[What changes for you](#what-changes-for-you) if you assert on generated DDL, subclass `Migrator`, or
read advisory lock holders.

## A half-built concurrent partitioned index can be finished

[#718](https://github.com/JasperFx/weasel/pull/718), contributed by
[@lahma](https://github.com/lahma).

A concurrent index on a partitioned table is built in three steps: `CREATE INDEX ON ONLY parent`, then
per partition a `CREATE INDEX CONCURRENTLY` under a deterministic child name, then
`ALTER INDEX ... ATTACH PARTITION`. That build can stop partway through — a uniqueness violation, a
cancelled build, or a process that died between a build and its attach — and the parent index is then
left invalid.

Weasel already read that invalid parent as drift and rebuilt it by dropping it first
([#508](https://github.com/JasperFx/weasel/pull/508)). The drop is what fell short: **dropping a partitioned index removes only the children attached to it**, and the child of the
partition the run stopped at was never attached. So the rebuild reached that partition, asked for the
same name again, and failed:

```
Npgsql.PostgresException : 42P07: relation "idx_docs_body_two" already exists
```

On every run, because nothing ever removed the leftover. Each partition's step now begins with a
`DROP INDEX CONCURRENTLY IF EXISTS` for that child, so a database stuck in this state finishes the index
on its next apply. A leftover is rebuilt rather than adopted, since it may predate a change to the
definition.

## Advisory lock holders are read from the current database only

[#719](https://github.com/JasperFx/weasel/issues/719) /
[#725](https://github.com/JasperFx/weasel/pull/725).

`Weasel.Postgresql`'s `AdvisoryLock.FindHolderAsync` queried `pg_locks` without scoping it to a database.
The two halves of that are worth stating in the order that makes the consequence clear:

- A PostgreSQL advisory lock is taken on **the acquiring session's database**. Two databases on one
  server hold the same lock id independently, and neither blocks the other — `pg_try_advisory_lock` for
  an id a neighbouring database holds returns `true`.
- `pg_locks` is **server-wide**, and shows every database's advisory rows to any session.

So the read did not merely report something irrelevant: it reported a lock as **held, with a pid and an
application name to make it convincing, for a lock this node could have taken**. For a diagnostic whose
whole purpose is to answer "who owns this shard" when a projection agent stops with
`ProgressionProgressOutOfOrderException`, naming a backend in another database is the worst available
answer. Database-per-tenant deployments and parallel test lanes share a server and reuse lock ids, so
they hit it by construction.

`Weasel.SqlServer` never had this; its equivalent read already filtered on `DB_ID()`.

## A Firebird catalog read that raced a rewrite is read again

[#723](https://github.com/JasperFx/weasel/issues/723) /
[#728](https://github.com/JasperFx/weasel/pull/728).

Firebird keeps a view's, routine's and trigger's source in a BLOB, and a catalog row carries only the
BLOB's **id** — the client fetches the blob itself in a later round trip. A concurrent `CREATE OR ALTER`
replaces the record version the id came from, and the blob can be collected before that fetch. The read
then fails with `BLOB not found` (error 335544382) for something no reader did wrong, and the same read
a moment later succeeds.

It arrived as an intermittent failure of Weasel's own racing-appliers test on Firebird 5. The reason it
was not already handled is that **it is a read failure, not a statement failure**: Firebird's existing
retry classifier governs re-running a DDL statement, and a statement never produces this error. Adding
it there would have had a losing applier re-run its DDL in answer to a failed read.

Instead, `Migrator` gained `IsConcurrentIntrospectionFailure`, and
`SchemaMigration.DetermineAsync(conn, migrator, …)` now reads the whole set again — a fresh command
builder and a fresh delta list — up to three attempts with a jittered backoff. `FirebirdMigrator`
answers `true` for that one error number. **Every other provider answers `false`**, so no other provider
makes a second attempt and nothing else changes.

Firebird 3 does not reproduce it: its collector will not take that record version away under an open
read, measured up to forty rewrites. If a Firebird 3 server does produce the error under some load, it is
handled the same way.

## `Weasel.Core` no longer produces a trim warning

[#724](https://github.com/JasperFx/weasel/pull/724), contributed by
[@lahma](https://github.com/lahma).

`MultiCommandDataReader.GetFieldType(int)` overrode `DbDataReader.GetFieldType(int)` without repeating
the base method's `[return: DynamicallyAccessedMembers(PublicFields | PublicProperties)]`. A trimmed or
natively published application that keeps the type reported `IL2093`, surfacing as
`IL2104: Assembly 'Weasel.Core' produced trim warnings` under the default single-warning mode. Any
application that runs a compiled batch through `CommandBuilderExtensions.ExecuteReaderAsync` keeps it,
whichever database it talks to, because the choice of a multi-command reader is only made at run time.
Found through Quartz.NET's trim canary
([quartznet/quartznet#4129](https://github.com/quartznet/quartznet/issues/4129)).

The override now carries the attribute, and a test sweeps every override in `Weasel.Core`,
`Weasel.Storage` and the six provider assemblies for the same mistake.

## What changes for you

- **Generated PostgreSQL DDL for a concurrent index on a partitioned table carries new statements.**
  Each partition's step now begins with `DROP INDEX CONCURRENTLY IF EXISTS <schema>.<child>;`. If you
  assert on generated DDL strings for such an index, update the expected text. Nothing changes for a
  non-partitioned index, or for a partitioned index built non-concurrently.
- **`AdvisoryLock.FindHolderAsync` returns `null` where it used to name a holder in another database.**
  That previous answer was wrong, but if you have tooling that treated any non-null result as "someone
  holds this", it will now correctly see the lock as free.
- **`Migrator` has a new virtual member, `IsConcurrentIntrospectionFailure(Exception)`.** It has a
  default implementation returning `false`, so existing subclasses compile and behave exactly as before.
  Override it only if your provider can fail an introspection read because the catalog changed under it.
- **A Firebird migration may now make up to three introspection attempts** instead of one. A failure
  that is not `BLOB not found` still surfaces on the first attempt, unchanged.

Two internal CI fixes also landed, with no effect on the packages:
[#720](https://github.com/JasperFx/weasel/issues/720) removed a Postgres matrix leg that tested nothing
(the variable it set never reached the test process, and the property it would have set has been
obsolete and unread for some time), and
[#721](https://github.com/JasperFx/weasel/issues/721) put `Weasel.CommandLine.Tests` into a workflow —
it had been run by none.
