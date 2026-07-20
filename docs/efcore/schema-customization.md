# Schema Mapping Customization

The [table mapping](/efcore/table-mapping) is deliberately provider-neutral, and that neutral seam can't express everything a real integration needs — PostgreSQL table partitioning, provider-specific index methods, or extra bookkeeping tables that must be migrated alongside the entity tables. `EfSchemaMappingCustomization` is the escape hatch.

The motivating case is **Wolverine's conjoined multi-tenancy**: it needs to attach Weasel-managed tenant partitioning to the tables mapped from tenant-scoped EF entity types, *and* inject the partition control/registry tables into the same migration so everything is created together.

## The hook

`EfSchemaMappingCustomization` has two parts:

| Member | Type | Purpose |
|---|---|---|
| `CustomizeTable` | `Action<IEntityType, ITable>?` | Called for every table mapped from an EF entity type, **after** the standard mapping. Decorate the table — attach partitioning, adjust indexes, opt into drift detection — including by downcasting to the concrete provider `Table`. |
| `AdditionalObjects` | `IReadOnlyList<ISchemaObject>` | Extra schema objects (partition control/registry tables, sequences, ...) migrated **ahead of** the mapped entity tables, so anything the tables depend on exists first. |

The same `IEntityType` / `ITable` pair is presented on **every** mapping pass — both `CreateDatabase` and each `CreateMigrationAsync` — so your customization stays applied consistently across delta detection and DDL generation.

## Usage

Pass a customization to `CreateMigrationAsync`. Here a tenant-scoped entity type gets Weasel-managed `LIST` partitioning, and a registry table is contributed to the migration ahead of the entity tables:

<!-- snippet: sample_efcore_schema_mapping_customization -->
<a id='snippet-sample_efcore_schema_mapping_customization'></a>
```cs
// A control/registry table that must be migrated *ahead* of the
// entity tables that depend on it
var partitionRegistry = new PgTable("tenants.partition_registry");
partitionRegistry.AddColumn<string>("tenant_id").AsPrimaryKey();
partitionRegistry.AddColumn<string>("partition_suffix").NotNull();

var customization = new EfSchemaMappingCustomization
{
    // Called for every table mapped from an EF entity type, after the
    // standard mapping. Downcast to the concrete provider Table to reach
    // provider-specific features the neutral seam can't express.
    CustomizeTable = (IEntityType entityType, ITable table) =>
    {
        if (typeof(ITenantScoped).IsAssignableFrom(entityType.ClrType)
            && table is PgTable pgTable)
        {
            // Attach Weasel-managed LIST partitioning on tenant_id
            pgTable.PartitionByList("tenant_id")
                .AddPartition("acme", "acme")
                .AddPartition("globex", "globex");
        }
    },

    // Extra schema objects migrated ahead of the entity tables
    AdditionalObjects = new ISchemaObject[] { partitionRegistry }
};

// The customization flows through delta detection and DDL generation
await using var migration =
    await serviceProvider.CreateMigrationAsync(dbContext, customization, ct);

if (migration.Migration.Difference != SchemaPatchDifference.None)
{
    await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, ct);
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreCustomizationSamples.cs#L26-L62' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_schema_mapping_customization' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The `CustomizeTable` delegate downcasts the neutral `ITable` to the concrete `Weasel.Postgresql.Tables.Table` to reach `PartitionByList(...)` — the same technique used for Npgsql `HasMethod("gin")` indexes or descending sort order that the neutral seam doesn't model. See [PostgreSQL partitioning](/postgresql/partitioning) for the partitioning APIs.

## Where it plugs in

Customization-aware overloads exist on all three entry points; the original no-customization overloads simply forward with `null`:

- `IServiceProvider.CreateMigrationAsync(context, customization, cancellation)` — detect and apply changes.
- `IServiceProvider.CreateDatabase(context, customization, identifier?)` — build an `IDatabaseWithTables`.
- `DbContextExtensions.GetSchemaObjectsForMigration(context, migrator, customization)` — the lower-level list of schema objects (sequences + additional objects + mapped tables, in dependency order).

<!-- snippet: sample_efcore_customization_create_database -->
<a id='snippet-sample_efcore_customization_create_database'></a>
```cs
var customization = new EfSchemaMappingCustomization
{
    CustomizeTable = (entityType, table) =>
    {
        // e.g. opt individual tables into drift detection
        table.DetectColumnDrift = true;
    }
};

// The same customization is applied on every mapping pass
var database = serviceProvider.CreateDatabase(dbContext, customization);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreCustomizationSamples.cs#L67-L79' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_customization_create_database' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Ordering guarantees

Contributed `AdditionalObjects` are always emitted **before** the mapped entity tables, and (as with the default flow) model sequences come before everything. That means a partition control table, a lookup table, or a sequence referenced by a mapped table's default is guaranteed to exist by the time the entity tables are created.

## Notes

- `AdditionalObjects` that implement `ITable` are registered with the `IDatabase` in `CreateDatabase`, so they participate in delta detection like any other Weasel table. Objects that are not tables flow through the `GetSchemaObjectsForMigration` / migration path.
- Because the hook hands you the real `ITable`, anything you can do to a Weasel table — [drift detection](/efcore/table-mapping#column-drift-detection), check constraints, extra indexes — is available here per table.
- The customization is a plain object with no framework coupling; construct it wherever you compose your migration and pass it through.
