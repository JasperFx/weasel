# db-dump

Dumps the entire DDL for all configured database objects to a file or directory.

## Usage

Dump to a single file:

```bash
dotnet run -- db-dump schema.sql
```

Split into separate files per feature:

```bash
dotnet run -- db-dump scripts/ --by-feature
```

## Options

| Option | Description |
|--------|-------------|
| `--by-feature` / `-f` | Split output into separate files, one per feature |
| `--transactional-script` | Wrap the generated SQL in a transaction |

## When to Use

Use `db-dump` when you need to:

- Document the full database schema as SQL.
- Generate an initial migration script for a new environment.
- Compare schema definitions between environments.
- Provide a complete DDL reference for review.

## Legacy Name

In Marten versions prior to V5.0, this command was called `marten-dump`.
