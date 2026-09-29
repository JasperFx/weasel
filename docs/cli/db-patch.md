# db-patch

Generates migration SQL patch files and corresponding rollback files for any pending schema changes.

## Usage

```bash
dotnet run -- db-patch <filename>
```

Example:

```bash
dotnet run -- db-patch migrations/patch001.sql
```

This generates two files:

- `migrations/patch001.sql` -- the forward migration SQL.
- `migrations/patch001.drop` -- the rollback SQL to undo the migration.

On SQLite, a change that rebuilt a table (a column type, a foreign key, a primary key) is undone by rebuilding it back into its previous shape, so the rollback keeps the rows. Like the forward file, the script does not switch off foreign key enforcement around the rebuild, and does not set `legacy_alter_table`, so its final rename fails with `error in view ...` when a view names the table; `SchemaMigration.RollbackAllAsync` handles both, as `ApplyAllAsync` does going forward.

## Options

| Option | Description |
|--------|-------------|
| `--transactional-script` | Wrap the generated SQL in a transaction |
| `--auto-create` | Override the AutoCreate behavior (default: `CreateOrUpdate`) |

## When to Use

Use `db-patch` when you need to:

- Have a DBA review migration SQL before it runs against production.
- Store migrations in version control alongside application code.
- Apply migrations through a separate deployment tool or process.
- Audit what changes will be made before applying them.

## Legacy Name

In Marten versions prior to V5.0, this command was called `marten-patch`.
