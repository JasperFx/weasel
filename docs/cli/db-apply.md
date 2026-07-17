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

For the same reason, a database whose apply fails with a *transient connection refusal* -- PostgreSQL `53300 too_many_connections`, `53400`, or `57P03 cannot_connect_now` -- is retried twice with an exponential backoff (3 attempts in total), so ambient connection pressure doesn't fail an entire deployment step. Migration failures themselves are **not** retried and fail the command immediately.

## Idempotency

`db-apply` is safe to run multiple times. If the database already matches the expected state, no changes are made.

## Legacy Name

In Marten versions prior to V5.0, this command was called `marten-apply`.
