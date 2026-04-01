# JSON Columns

Starting with Weasel 8.11.1, the EF Core integration supports JSON column mappings defined via `OwnsOne().ToJson()`. This was previously silently ignored, causing JSON columns to be missing from Weasel's schema model ([GitHub issue #232](https://github.com/JasperFx/weasel/issues/232)).

## How It Works

When EF Core maps a complex property to a JSON column using `ToJson()`, Weasel detects this during table mapping by iterating each entity type's navigations. For each navigation whose target entity type returns `true` from `IsMappedToJson()`, Weasel:

1. Reads the column name via `GetContainerColumnName()`.
2. Reads the column type via `GetContainerColumnType()`, defaulting to `jsonb` if not specified.
3. Sets nullability based on whether the navigation's foreign key is marked as required.

## EF Core Configuration

Define a JSON column using `ComplexProperty` with `ToJson()`:

<!-- snippet: sample_efcore_json_column_configuration -->
<a id='snippet-sample_efcore_json_column_configuration'></a>
```cs
public class OrderDbContext : DbContext
{
    public OrderDbContext(DbContextOptions<OrderDbContext> options) : base(options)
    {
    }

    public DbSet<Order> Orders { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("orders", "myschema");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Status).HasColumnName("status");
            entity.OwnsOne(x => x.ShippingAddress, b =>
            {
                b.ToJson("shipping_address");
            });
        });
    }
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreSamples.cs#L128-L152' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_json_column_configuration' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The resulting Weasel table will include three columns:

- `id` -- primary key
- `status` -- text column
- `shipping_address` -- `jsonb` column, non-nullable (because `IsRequired()` was called)

## Column Type

The column type defaults to `jsonb` for PostgreSQL. If EF Core specifies a different container column type via its metadata, that type is used instead. For SQL Server, this would typically be `nvarchar(max)`.

## Nullability

The JSON column's nullability is determined by the navigation's foreign key `IsRequired` setting:

- `b.IsRequired()` -- column is NOT NULL
- No `IsRequired()` call -- column allows NULL

## Owned Types Exclusion

Owned entity types (those configured via `OwnsOne()` or `OwnsMany()`) are excluded from `GetEntityTypesForMigration()` since they do not produce their own tables. This fix was added in Weasel 8.11.2 to address [GitHub issue #234](https://github.com/JasperFx/weasel/issues/234), which caused errors when owned types were incorrectly treated as standalone tables.

## Migration Example

<!-- snippet: sample_efcore_json_migration_example -->
<a id='snippet-sample_efcore_json_migration_example'></a>
```cs
await using var migration = await serviceProvider.CreateMigrationAsync(dbContext, ct);

// The migration will include the JSON column in the table definition.
// If the column was previously missing (pre-8.11.1), the delta detection
// will flag it as a new column to add.
if (migration.Migration.Difference != SchemaPatchDifference.None)
{
    await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, ct);
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/EfCoreSamples.cs#L114-L124' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_efcore_json_migration_example' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->
