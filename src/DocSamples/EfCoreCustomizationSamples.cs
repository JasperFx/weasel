using JasperFx;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Weasel.Core;
using Weasel.EntityFrameworkCore;
using Weasel.Postgresql;
using ITable = Weasel.Core.ITable;
using PgTable = Weasel.Postgresql.Tables.Table;

namespace DocSamples;

// A marker interface an application (e.g. Wolverine's conjoined multi-tenancy)
// might use to opt entity types into tenant partitioning
public interface ITenantScoped
{
}

public class EfCoreCustomizationSamples
{
    private IServiceProvider serviceProvider = null!;
    private DbContext dbContext = null!;
    private CancellationToken ct;

    public async Task customize_mapped_tables_and_contribute_objects()
    {
        #region sample_efcore_schema_mapping_customization
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
        #endregion
    }

    public async Task allow_drops_on_ef_derived_tables()
    {
        #region sample_efcore_allow_drops
        // EF-derived tables are add-only by default: a column, index or foreign key the mapper
        // could not translate is left in place rather than dropped (weasel#629). Set AllowDrops
        // when the EF model really is the whole truth about these tables.
        var customization = new EfSchemaMappingCustomization { AllowDrops = true };

        await using var migration =
            await serviceProvider.CreateMigrationAsync(dbContext, customization, ct);
        await migration.ExecuteAsync(AutoCreate.CreateOrUpdate, ct);
        #endregion
    }

    public void allow_drops_on_one_table_only()
    {
        #region sample_efcore_allow_drops_per_table
        var customization = new EfSchemaMappingCustomization
        {
            // CustomizeTable runs AFTER the mapping, so it has the last word on the policy
            CustomizeTable = (entityType, table) =>
            {
                if (entityType.ClrType.Name == "StagingRow")
                {
                    table.AddOnlyMigrations = false;
                }
            }
        };

        var database = serviceProvider.CreateDatabase(dbContext, customization);
        #endregion
    }

    public void customize_when_building_a_database()
    {
        #region sample_efcore_customization_create_database
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
        #endregion
    }
}
