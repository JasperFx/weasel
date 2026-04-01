# db-list

Lists all configured databases and their identifiers.

## Usage

```bash
dotnet run -- db-list
```

## Output

The command displays a table with the following columns:

| Column | Description |
|--------|-------------|
| DatabaseUri | The unique identifier for the database |
| SubjectUri | The subject URI associated with the database |
| TenantId(s) | The tenant identifiers associated with the database |

## When to Use

Use `db-list` to discover the database identifiers registered in your application. These identifiers can then be passed to the `--database` / `-d` flag on other commands to target a specific database.

This is especially helpful in multi-tenant or multi-database setups where you need to identify which databases are configured before running migrations or assertions against a specific one.
