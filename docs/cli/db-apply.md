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

## Idempotency

`db-apply` is safe to run multiple times. If the database already matches the expected state, no changes are made.

## Legacy Name

In Marten versions prior to V5.0, this command was called `marten-apply`.
