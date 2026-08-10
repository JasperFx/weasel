# db-assert

Asserts that the existing database(s) match the current configuration. Exits with a non-zero exit code if any differences are found.

## Usage

```bash
dotnet run -- db-assert
```

Filter to a specific database:

```bash
dotnet run -- db-assert -d MyDatabase
```

## Behavior

This command calls `AssertDatabaseMatchesConfigurationAsync()` on each discovered database. Every database is checked -- a failure never stops the rest of the walk -- and each failing database's `DatabaseValidationException` is printed as it happens. If any database failed, the process exits with a non-zero exit code.

Like [db-apply](/cli/db-apply), the walk honors `--parallel` for fleets of databases:

```bash
dotnet run -- db-assert --parallel 8
```

Targets sharing a `DatabaseUri` (the same physical database) are always checked sequentially within the group; the parallelism counts physical databases in flight. The default is `1`, strictly sequential.

## CI/CD Usage

`db-assert` is designed for use in CI/CD pipelines to verify database state before deployment:

```bash
# Fail the pipeline if the database is out of sync
dotnet run -- db-assert
if [ $? -ne 0 ]; then
  echo "Database schema does not match application configuration!"
  exit 1
fi
```

This lets you catch schema drift early and ensure that migrations have been applied before deploying new application code.

## Legacy Name

In Marten versions prior to V5.0, this command was called `marten-assert`.
