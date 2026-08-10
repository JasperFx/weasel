# db-apply

Applies all outstanding changes to the database(s) based on the current configuration.

## Usage

```bash
dotnet run -- db-apply
```

Filter to a specific database:

```bash
dotnet run -- db-apply -d MyDatabase
```

## Behavior

This command calls `ApplyAllConfiguredChangesToDatabaseAsync()` on each discovered database. It compares the expected schema (defined in code) against the actual database state and applies any necessary DDL changes.

The command reports one of:

- **No changes needed** -- the database already matches the configuration.
- **Successfully applied migrations** -- changes were detected and applied.

## Many databases

Before anything can be applied, the databases have to be discovered -- each registered `IDatabaseSource` is asked to build its list, and for a sharded tenancy source that is real work, not bookkeeping. Discovery is therefore announced before it starts and reported per source as it goes:

```
Discovering databases...
  MartenDatabaseSource: 512 databases in 28.1s
Found 1037 databases in 30.6s
```

The per-source line is the useful one when discovery is slow: it says which tenancy source the time went to, which is otherwise invisible from the command. This reporting applies to every command that resolves databases, not just `db-apply`.

Databases are then applied one at a time by default, and each one's progress is reported as `(n/total)` so a long walk over a sharded or multi-tenanted store can be watched. The counter counts *completions*, incremented as each database finishes.

### Parallelism

At fleet scale the sequential walk is the deployment cost: 1,037 databases at ~0.5s apiece is 8m41s of wall time even when every one of them reports `No changes detected`. The `--parallel` flag bounds how many databases are applied concurrently:

```bash
dotnet run -- db-apply --parallel 8
```

Details worth knowing:

- **The unit is the physical database.** Targets are grouped by their `DatabaseUri`, the parallelism applies *across* groups, and a group is always applied sequentially *within* itself. Parallel DDL against the same physical database only contends on its locks, so `--parallel 8` means "8 physical databases in flight" -- which is also the right number to reason about against the server's `max_connections` ceiling. (With connection-pool release per finished database, peak usage stays at roughly the pools actually in flight.)
- **Output stays attributable.** When running in parallel, each database's migration DDL is buffered and flushed as a single block directly above that database's completion line, instead of interleaving line by line with other appliers. At `--parallel 1` (the default) the DDL still streams live as it always has -- on a genuinely long migration the streaming SQL is how an operator sees the run is alive.
- **Failures never stop the rest, at any parallelism.** Every database is attempted; each failure is reported when it happens; and the command ends with a summary block (`N unchanged, M migrated, K failed`, failures listed) and, if anything failed, throws an `AggregateException` carrying every per-database failure with its original stack trace -- which is what makes the process exit code non-zero. This is deliberately *not* a function of the parallelism value: sequential runs aggregate too, rather than failing fast on the first bad database.

The default is `1` -- strictly sequential, exactly the behavior before the flag existed. `db-assert` honors the same flag with the same physical-database grouping.

Because `db-apply` is a one-shot command that owns its data sources, each database's connection pool is released as soon as that database is done, rather than being left to age out on its own idle lifetime. Peak connection usage therefore stays at roughly the one database being applied, instead of trailing an idle pool per recently-applied database -- which matters when applying across hundreds of databases on a server that is near `max_connections`.

For the same reason, a database whose apply fails with a *transient connection refusal* is retried twice with an exponential backoff (3 attempts in total), so ambient connection pressure doesn't fail an entire deployment step. Migration failures themselves are **not** retried -- they are reported when they happen, the remaining databases are still applied, and the command fails at the end with an `AggregateException` over every per-database failure.

What counts as a transient refusal is decided per provider:

| Provider | Retried on |
|----------|------------|
| PostgreSQL | `53300` too_many_connections, `53400`, `57P03` cannot_connect_now |
| SQL Server | `17809` too many user connections, and the Azure SQL resource/throttling set (`10928`/`10929`/`40197`/`40501`/`40613`/`49918`-`49920`) |
| MySQL | `1040` too many connections, `1203`, `1226`, unable to connect to host |
| Oracle | `ORA-00020` max processes exceeded, `ORA-12516`/`12518`/`12520` listener busy |
| SQLite | N/A -- no server, so no connection ceiling to hit |

The list is deliberately narrow, because a wrong retry is worse than a missed one: a missed code just means today's behavior (fail immediately), while a false positive silently re-runs a migration that genuinely failed. So errors that can't be told apart from a failed migration are excluded even when they look connection-ish -- SQL Server's `-2` ("timeout expired") is also raised for *command* timeouts, so a slow `CREATE INDEX` would otherwise be retried as though the server had refused it, and transport-level drops (`10053`/`10054`, `ORA-12537`/`12570`) are reported identically whether they happened while connecting or midway through a statement. Bad credentials, deadlocks and DDL mistakes are excluded for the same reason.

::: tip
Pool release is scoped differently by provider. PostgreSQL clears the specific `NpgsqlDataSource`, while the other providers' drivers key their pools by connection string within the process -- so releasing there also drops idle connections held elsewhere in the same process for that connection string. Connections in use are never killed (they're discarded when returned), and for a one-shot CLI like `db-apply` this makes no difference, but it's worth knowing if you call `ReleaseConnectionPoolAsync()` yourself from inside a running application.
:::

## Idempotency

`db-apply` is safe to run multiple times. If the database already matches the expected state, no changes are made.

## Legacy Name

In Marten versions prior to V5.0, this command was called `marten-apply`.
