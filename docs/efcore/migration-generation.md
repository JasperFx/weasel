# EF Core Migration Generation

Weasel can generate **EF Core migration files from its own schema model** — the reverse direction of [table mapping](/efcore/table-mapping). Instead of Weasel applying schema changes itself (`db-patch` / `db-apply`), it emits standard, compilable EF Core migration artifacts that your team applies with the tools it already knows: `dotnet ef database update`, idempotent SQL scripts, and migration bundles.

## When to use which flow

| | Weasel-native (`db-patch` / `db-apply`) | EF migration generation (`db-ef-migration`) |
|---|---|---|
| Schema is applied by | Weasel at startup or CLI | EF Core toolchain (`dotnet ef`, bundles, scripts) |
| Change history | none (delta against live DB) | versioned migration files + `__EFMigrationsHistory` |
| DBA review artifact | patch SQL file | migration `.cs` files / idempotent script |
| Best for | dev loops, Marten-style auto-migration | teams standardizing on EF migrations for deployment |

Both flows read the same source of truth: **`IDatabase.AllObjects()`** — Marten system tables, Wolverine envelope storage, Polecat event storage, and [EF-projection tables](/efcore/table-mapping) all flow through one door.

## The generated artifacts

`db-ef-migration add <Name>` (or `EfMigrationGenerator.AddAsync`) writes three kinds of files:

1. **Migration classes** (`<timestamp>_<Name>.cs`) — attribute-only `Migration` subclasses carrying `[DbContext]` and `[Migration]` attributes with real `Up()` **and** `Down()` bodies over the public `MigrationBuilder` surface. There is deliberately no `BuildTargetModel` body: the empty target model is fully supported by the EF toolchain (runtime `Migrate()`, CLI update, `--idempotent` scripts, and bundles were all verified end-to-end on EF 9 and EF 10).
2. **A stub `DbContext`** (once per `IDatabase`) — no entities; provider configured; the `__EFMigrationsHistory` table **relocated into the critter-stack schema** so it never collides with your application's own EF context; the EF 9+ `PendingModelChangesWarning` suppressed; plus an `IDesignTimeDbContextFactory` reading the `WEASEL_EF_CONNECTION` environment variable so the EF CLI works without an application host.
3. **A schema snapshot** (`weasel-schema-snapshot.json`) — Weasel's analog of EF's `ModelSnapshot`: design-time JSON written beside the migrations and never compiled. It is the baseline the next `add` diffs against.

## Getting started

Generate the first migration for the selected `IDatabase` (same `--database` selection UX as `db-patch`):

```bash
dotnet run -- db-ef-migration add Initial
```

Programmatically:

<!-- snippet: sample_efgen_add_migration -->
<a id='snippet-sample_efgen_add_migration'></a>
```cs
// IDatabase is the source of all schema objects — Marten system
// tables, Wolverine envelope storage, EF projection tables, ...
var result = await EfMigrationGenerator.AddAsync(
    database,
    "Initial",
    new EfMigrationGenerationOptions
    {
        Directory = "WeaselMigrations",
        Namespace = "MyApp.WeaselMigrations"
    });

// first run writes three artifacts:
// result.MigrationFile  -> 20260718120000_Initial.cs (attribute-only migration)
// result.ContextFile    -> <Identifier>SchemaDbContext.cs (stub context)
// result.SnapshotFile   -> weasel-schema-snapshot.json (design-time baseline)
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreMigrationSamples.cs#L14-L30' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efgen_add_migration' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Compile the generated files into a project that references the EF provider package, then apply them exactly like any EF migration:

```bash
export WEASEL_EF_CONNECTION="Host=localhost;Database=app;..."
dotnet ef database update --context MyStoreSchemaDbContext
```

Idempotent scripts and bundles work the same way:

```bash
dotnet ef migrations script --idempotent --context MyStoreSchemaDbContext -o migrations.sql
dotnet ef migrations bundle --context MyStoreSchemaDbContext
```

## Incremental migrations

The next `db-ef-migration add <Name>` diffs the current model against the snapshot **entirely in memory** — no live database, no shadow container — and emits an incremental migration (`AddColumn` / `AlterColumn` / `DropColumn`, index and foreign-key recreation, primary-key changes, sequence changes) with a real reverse-ordered `Down()`. Migration ids are `yyyyMMddHHmmss_Name` with a monotonicity guard, since EF orders migrations by plain string sort of the id.

<!-- snippet: sample_efgen_snapshot_diff -->
<a id='snippet-sample_efgen_snapshot_diff'></a>
```cs
var options = new MigrationOperationTranslationOptions(EfMigrationProvider.PostgreSql)
{
    Migrator = new PostgresqlMigrator()
};

var table = new PgTable("app.orders");
table.AddColumn<int>("id").AsPrimaryKey();
table.AddColumn<string>("name").NotNull();

// the serialized snapshot is Weasel's analog of EF's ModelSnapshot:
// design-time JSON written beside the migrations, never compiled
var baseline = EfSchemaSnapshot.FromSchemaObjects(new ISchemaObject[] { table }, options);
var json = baseline.ToJson();

// ... later: the model changed
table.AddColumn<string>("tenant_id").NotNull();
table.ColumnFor("tenant_id")!.DefaultExpression = "'*DEFAULT*'";
var target = EfSchemaSnapshot.FromSchemaObjects(new ISchemaObject[] { table }, options);

// diff entirely in memory — no live database, no shadow container
var incremental = EfSnapshotDiffer.Diff(EfSchemaSnapshot.FromJson(json), target, options);
// incremental.UpOperations   -> AddColumn tenant_id
// incremental.DownOperations -> DropColumn tenant_id
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreMigrationSamples.cs#L60-L84' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efgen_snapshot_diff' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Pass `--against-database` to use the secondary **live-database baseline mode**: Weasel's own delta detection runs against the actual database and the resulting migration SQL (and rollback SQL) is wrapped in `Sql()` operations. This mode handles everything Weasel can migrate, including the cases the snapshot diff deliberately refuses:

<!-- snippet: sample_efgen_live_database_diff -->
<a id='snippet-sample_efgen_live_database_diff'></a>
```cs
// the secondary mode: let Weasel's own delta detection diff against
// the actual database, and wrap the migration SQL in Sql() operations.
// Handles everything Weasel can migrate — including partition deltas
// and function changes the snapshot diff refuses.
var operations = await EfSnapshotDiffer.DiffAgainstDatabaseAsync(database);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreMigrationSamples.cs#L89-L95' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efgen_live_database_diff' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Adopting an existing database

For a database that already has the schema (a running Marten/Wolverine application), record the generated migrations as applied without executing anything — the EF-sanctioned baselining technique:

```bash
dotnet run -- db-ef-migration baseline
```

<!-- snippet: sample_efgen_baseline -->
<a id='snippet-sample_efgen_baseline'></a>
```cs
// adopt a pre-existing database: record every generated migration file
// as already applied (history rows only, nothing is executed)
var recorded = await EfMigrationGenerator.BaselineAsync(
    database,
    new EfMigrationGenerationOptions { Directory = "WeaselMigrations" });
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreMigrationSamples.cs#L100-L106' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efgen_baseline' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Afterwards `dotnet ef database update` reports nothing pending, and future incremental migrations apply on top.

## The translation layer

Under the CLI sits a public API: Weasel schema objects translate into EF `MigrationOperation` instances with **raw store type strings everywhere**, so the DDL EF generates matches Weasel's own byte-for-byte — identity columns become the proper provider annotations, computed columns become `ComputedColumnSql`, cascade actions map onto `ReferentialAction` (with SQL Server's `Restrict` ≡ `NO ACTION` normalization), and schemas get `EnsureSchema` operations.

<!-- snippet: sample_efgen_translate_table -->
<a id='snippet-sample_efgen_translate_table'></a>
```cs
var table = new PgTable("app.orders");
table.AddColumn<int>("id").AsPrimaryKey();
table.AddColumn<string>("name").NotNull();

// translate Weasel schema objects into EF Core MigrationOperation
// instances — raw store types everywhere, so the DDL EF generates
// matches Weasel's own
var options = new MigrationOperationTranslationOptions(EfMigrationProvider.PostgreSql)
{
    Migrator = new PostgresqlMigrator()
};

var operations = new ISchemaObject[] { table }.ToMigrationOperations(options);
var downOperations = new ISchemaObject[] { table }.ToDropMigrationOperations(options);

// render as a compilable, attribute-only migration file
var migration = EfMigrationFileEmitter.EmitMigration(
    "AddOrders", operations, downOperations,
    new EfMigrationEmissionOptions("AppSchemaDbContext"));
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreMigrationSamples.cs#L35-L55' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efgen_translate_table' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Limitations and raw-SQL fallbacks

Everything EF cannot model routes through `migrationBuilder.Sql(...)` blocks carrying Weasel's own DDL, so these work from day one:

- **PostgreSQL table partitioning** (RANGE/LIST/HASH and the managed strategies) — partitioned tables are detected automatically and emitted as raw DDL (the Npgsql EF provider has no partitioning model).
- **PL/pgSQL functions, SQL Server stored procedures and table types**.
- **Expression indexes**, and **SQL Server unique indexes without a filter** (EF's SqlServer generator would otherwise add a spurious `WHERE ... IS NOT NULL` filter, because an attribute-only migration has no model to prove the columns non-nullable).

Deliberate boundaries:

- `dotnet ef migrations add` / `remove` are **not supported against the stub context** — Weasel authors the migrations; the EF scaffolder needs the model snapshot the stub deliberately doesn't have. Use `db-ef-migration add`.
- Changed raw-SQL objects (a partition layout change, a rewritten function body) are refused by the snapshot diff with guidance — generate that migration with `--against-database` or author it by hand.
- Renames are not inferred from the model (Weasel carries no rename intent); today a rename diffs as drop + add.
- v1 providers are **PostgreSQL and SQL Server**. SQLite is out: its ALTER-emulation rebuilds tables from the migration's target model, which attribute-only migrations don't carry.
- The `PendingModelChangesWarning` suppression baked into the stub context is defensive: with no `ModelSnapshot` at all the EF 9+ pending-changes check has nothing to fire on, but the suppression protects anyone who later adds entities to the same context.
