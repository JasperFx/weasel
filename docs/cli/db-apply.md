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

Databases are applied one at a time, in sequence, and each one's progress is reported as `(n/total)` so a long walk over a sharded or multi-tenanted store can be watched.

Because `db-apply` is a one-shot command that owns its data sources, each database's connection pool is released as soon as that database is done, rather than being left to age out on its own idle lifetime. Peak connection usage therefore stays at roughly the one database being applied, instead of trailing an idle pool per recently-applied database -- which matters when applying across hundreds of databases on a server that is near `max_connections`.

For the same reason, a database whose apply fails with a *transient connection refusal* is retried twice with an exponential backoff (3 attempts in total), so ambient connection pressure doesn't fail an entire deployment step. Migration failures themselves are **not** retried and fail the command immediately.

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
