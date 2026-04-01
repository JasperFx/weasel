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

This command calls `AssertDatabaseMatchesConfigurationAsync()` on each discovered database. If the actual database state differs from the expected configuration, it throws a `DatabaseValidationException` and the process exits with a non-zero exit code.

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
