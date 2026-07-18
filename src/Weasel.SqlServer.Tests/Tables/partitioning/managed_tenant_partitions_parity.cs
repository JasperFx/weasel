using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables.partitioning;

/// <summary>
///     ManagedTenantPartitions parity with the PostgreSQL ManagedListPartitions
///     feature set (#362): data-removing drop semantics, tenant bucketing via
///     explicit ordinals, new-table back-fill, and per-table status reporting.
/// </summary>
[Collection("integration")]
public class managed_tenant_partitions_parity: IntegrationContext
{
    public managed_tenant_partitions_parity() : base("mtpp")
    {
    }

    // ---------------------------------------------------------------------
    // Unit — no database
    // ---------------------------------------------------------------------

    [Fact]
    public void ordinal_sharing_removes_the_unique_registry_index()
    {
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtpp", "tenant_registry"));

        var registry = (Table)((Weasel.Core.Migrations.IFeatureSchema)manager).Objects[0];
        registry.Indexes.ShouldContain(x => x.Name == "uniq_tenant_registry_ordinal");
        manager.AllowOrdinalSharing.ShouldBeFalse();

        manager.AllowOrdinalSharing = true;
        registry.Indexes.ShouldNotContain(x => x.Name == "uniq_tenant_registry_ordinal");

        manager.AllowOrdinalSharing = false;
        registry.Indexes.ShouldContain(x => x.Name == "uniq_tenant_registry_ordinal");
    }

    // ---------------------------------------------------------------------
    // Integration
    // ---------------------------------------------------------------------

    private async Task ResetSchemaAndPartitionObjects()
    {
        await ResetSchema();

        foreach (var pfName in new[] { "pf_porders_tenant_ordinal", "pf_pevents_tenant_ordinal" })
        {
            var psName = pfName.Replace("pf_", "ps_");
            await theConnection.CreateCommand($@"
IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{psName}')
    DROP PARTITION SCHEME [{psName}];
IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{pfName}')
    DROP PARTITION FUNCTION [{pfName}];")
                .ExecuteNonQueryAsync();
        }
    }

    private Table theOrdersTable(ManagedTenantPartitions manager)
    {
        var orders = new Table("mtpp.porders");
        orders.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.PartitionByManagedTenants(manager);
        return orders;
    }

    private Table theEventsTable(ManagedTenantPartitions manager)
    {
        var events = new Table("mtpp.pevents");
        events.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        events.AddColumn<int>("id").AsPrimaryKey();
        events.PartitionByManagedTenants(manager);
        return events;
    }

    private ManagedTenantPartitions theManager() =>
        new("tenants", new DbObjectName("mtpp", "tenant_registry"));

    [Fact]
    public async Task drop_with_delete_data_removes_the_tenant_rows_before_merging()
    {
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtpp_integration", ConnectionSource.ConnectionString);
        var manager = theManager();
        var orders = theOrdersTable(manager);
        database.AddTable(orders);

        await CreateSchemaObjectInDatabase(orders);

        await manager.AddPartitionToAllTables(database, "acme", CancellationToken.None);
        await manager.AddPartitionToAllTables(database, "globex", CancellationToken.None);

        await theConnection.CreateCommand(
                "INSERT INTO mtpp.porders (tenant_ordinal, id) VALUES (1, 1), (1, 2), (2, 3)")
            .ExecuteNonQueryAsync();

        await manager.DropPartitionFromAllTables(
            NullLogger.Instance, database, new[] { "acme" }, TenantDropBehavior.DeleteData,
            CancellationToken.None);

        // acme's rows (ordinal 1) are gone, globex's (ordinal 2) survive
        var remaining = (int)(await theConnection.CreateCommand(
            "SELECT COUNT(*) FROM mtpp.porders").ExecuteScalarAsync())!;
        remaining.ShouldBe(1);

        var ordinalLeft = (int)(await theConnection.CreateCommand(
            "SELECT tenant_ordinal FROM mtpp.porders").ExecuteScalarAsync())!;
        ordinalLeft.ShouldBe(2);
    }

    [Fact]
    public async Task drop_with_retain_data_keeps_the_tenant_rows()
    {
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtpp_integration", ConnectionSource.ConnectionString);
        var manager = theManager();
        var orders = theOrdersTable(manager);
        database.AddTable(orders);

        await CreateSchemaObjectInDatabase(orders);

        await manager.AddPartitionToAllTables(database, "acme", CancellationToken.None);
        await manager.AddPartitionToAllTables(database, "globex", CancellationToken.None);

        await theConnection.CreateCommand(
                "INSERT INTO mtpp.porders (tenant_ordinal, id) VALUES (1, 1), (1, 2), (2, 3)")
            .ExecuteNonQueryAsync();

        await manager.DropPartitionFromAllTables(
            NullLogger.Instance, database, new[] { "acme" }, CancellationToken.None);

        // historical behavior: merge only, rows are retained
        var remaining = (int)(await theConnection.CreateCommand(
            "SELECT COUNT(*) FROM mtpp.porders").ExecuteScalarAsync())!;
        remaining.ShouldBe(3);
    }

    [Fact]
    public async Task explicit_ordinals_bucket_multiple_tenants_into_one_partition()
    {
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtpp_integration", ConnectionSource.ConnectionString);
        var manager = theManager();
        manager.AllowOrdinalSharing = true;
        var orders = theOrdersTable(manager);
        database.AddTable(orders);

        await CreateSchemaObjectInDatabase(orders);

        var result = await manager.AddPartitionsToAllTables(
            NullLogger.Instance, database,
            new Dictionary<string, int> { ["acme"] = 1, ["globex"] = 1, ["initech"] = 2 },
            CancellationToken.None);

        result.Ordinals["acme"].ShouldBe(1);
        result.Ordinals["globex"].ShouldBe(1);
        result.Ordinals["initech"].ShouldBe(2);
        result.Tables.ShouldContain(x =>
            x.Identifier.QualifiedName == "mtpp.porders" && x.Status == PartitionMigrationStatus.Complete);

        // three tenants, two partitions
        var boundaries = await ReadBoundariesAsync("pf_porders_tenant_ordinal");
        boundaries.ShouldBe(new[] { 0, 1, 2 });

        manager.Ordinals.Count.ShouldBe(3);
    }

    [Fact]
    public async Task explicit_ordinal_collision_throws_without_sharing_enabled()
    {
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtpp_integration", ConnectionSource.ConnectionString);
        var manager = theManager();
        var orders = theOrdersTable(manager);
        database.AddTable(orders);

        await CreateSchemaObjectInDatabase(orders);

        await manager.AddPartitionToAllTables(
            NullLogger.Instance, database, "acme", 1, CancellationToken.None);

        await Should.ThrowAsync<InvalidOperationException>(() =>
            manager.AddPartitionToAllTables(
                NullLogger.Instance, database, "globex", 1, CancellationToken.None));
    }

    [Fact]
    public async Task remapping_a_tenant_to_a_different_ordinal_throws()
    {
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtpp_integration", ConnectionSource.ConnectionString);
        var manager = theManager();
        var orders = theOrdersTable(manager);
        database.AddTable(orders);

        await CreateSchemaObjectInDatabase(orders);

        await manager.AddPartitionToAllTables(
            NullLogger.Instance, database, "acme", 3, CancellationToken.None);

        // same ordinal is an idempotent no-op
        var again = await manager.AddPartitionToAllTables(
            NullLogger.Instance, database, "acme", 3, CancellationToken.None);
        again.ShouldBe(3);

        await Should.ThrowAsync<InvalidOperationException>(() =>
            manager.AddPartitionToAllTables(
                NullLogger.Instance, database, "acme", 5, CancellationToken.None));
    }

    [Fact]
    public async Task dropping_one_tenant_of_a_shared_ordinal_keeps_the_partition_and_data()
    {
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtpp_integration", ConnectionSource.ConnectionString);
        var manager = theManager();
        manager.AllowOrdinalSharing = true;
        var orders = theOrdersTable(manager);
        database.AddTable(orders);

        await CreateSchemaObjectInDatabase(orders);

        await manager.AddPartitionsToAllTables(
            NullLogger.Instance, database,
            new Dictionary<string, int> { ["acme"] = 1, ["globex"] = 1 },
            CancellationToken.None);

        await theConnection.CreateCommand(
                "INSERT INTO mtpp.porders (tenant_ordinal, id) VALUES (1, 1), (1, 2)")
            .ExecuteNonQueryAsync();

        // acme is dropped, but globex still owns ordinal 1 — no merge, no purge,
        // even with DeleteData requested
        await manager.DropPartitionFromAllTables(
            NullLogger.Instance, database, new[] { "acme" }, TenantDropBehavior.DeleteData,
            CancellationToken.None);

        manager.Ordinals.ShouldNotContainKey("acme");
        manager.Ordinals.ShouldContainKeyAndValue("globex", 1);

        (await ReadBoundariesAsync("pf_porders_tenant_ordinal")).ShouldBe(new[] { 0, 1 });

        var remaining = (int)(await theConnection.CreateCommand(
            "SELECT COUNT(*) FROM mtpp.porders").ExecuteScalarAsync())!;
        remaining.ShouldBe(2);

        // dropping the LAST tenant of the ordinal releases the partition and the rows
        await manager.DropPartitionFromAllTables(
            NullLogger.Instance, database, new[] { "globex" }, TenantDropBehavior.DeleteData,
            CancellationToken.None);

        (await ReadBoundariesAsync("pf_porders_tenant_ordinal")).ShouldBe(new[] { 0 });
        ((int)(await theConnection.CreateCommand(
            "SELECT COUNT(*) FROM mtpp.porders").ExecuteScalarAsync())!).ShouldBe(0);
    }

    [Fact]
    public async Task new_table_added_to_an_existing_managed_set_is_backfilled()
    {
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtpp_integration", ConnectionSource.ConnectionString);
        var manager = theManager();
        var orders = theOrdersTable(manager);
        database.AddTable(orders);

        await CreateSchemaObjectInDatabase(orders);

        await manager.AddPartitionToAllTables(database, "acme", CancellationToken.None);
        await manager.AddPartitionToAllTables(database, "globex", CancellationToken.None);

        // wire a NEW table to the existing managed set after tenants exist
        var events = theEventsTable(manager);
        database.AddTable(events);

        var statuses = await manager.MigrateAllTablesAsync(
            NullLogger.Instance, database, CancellationToken.None);

        statuses.ShouldContain(x =>
            x.Identifier.QualifiedName == "mtpp.pevents" && x.Status == PartitionMigrationStatus.Complete);

        // the new table has every registered ordinal
        (await ReadBoundariesAsync("pf_pevents_tenant_ordinal")).ShouldBe(new[] { 0, 1, 2 });
    }

    [Fact]
    public async Task new_table_created_by_regular_migration_gets_all_existing_ordinals()
    {
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtpp_integration", ConnectionSource.ConnectionString);
        var manager = theManager();
        var orders = theOrdersTable(manager);
        database.AddTable(orders);

        await CreateSchemaObjectInDatabase(orders);

        await manager.AddPartitionToAllTables(database, "acme", CancellationToken.None);
        await manager.AddPartitionToAllTables(database, "globex", CancellationToken.None);

        // a brand-new table created through the normal migration path picks up
        // the full boundary set from the manager's in-memory map
        var events = theEventsTable(manager);
        database.AddTable(events);
        await CreateSchemaObjectInDatabase(events);

        (await ReadBoundariesAsync("pf_pevents_tenant_ordinal")).ShouldBe(new[] { 0, 1, 2 });
    }

    [Fact]
    public async Task batch_add_reports_per_table_status()
    {
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtpp_integration", ConnectionSource.ConnectionString);
        var manager = theManager();
        var orders = theOrdersTable(manager);
        var events = theEventsTable(manager);
        database.AddTable(orders);
        database.AddTable(events);

        await CreateSchemaObjectInDatabase(orders);
        await CreateSchemaObjectInDatabase(events);

        var result = await manager.AddPartitionsToAllTables(
            NullLogger.Instance, database, new[] { "acme", "globex" }, CancellationToken.None);

        result.Ordinals.ShouldContainKeyAndValue("acme", 1);
        result.Ordinals.ShouldContainKeyAndValue("globex", 2);

        result.Tables.Length.ShouldBe(2);
        result.Tables.ShouldAllBe(x => x.Status == PartitionMigrationStatus.Complete);
        result.Tables.Select(x => x.Identifier.QualifiedName)
            .ShouldBe(new[] { "mtpp.porders", "mtpp.pevents" }, ignoreOrder: true);
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private async Task<int[]> ReadBoundariesAsync(string partitionFunctionName)
    {
        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = @"
SELECT CAST(prv.value AS int)
FROM sys.partition_functions pf
JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id
WHERE pf.name = @pf
ORDER BY prv.boundary_id;";
        cmd.Parameters.AddWithValue("@pf", partitionFunctionName);

        var list = new List<int>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(reader.GetInt32(0));
        }

        return list.ToArray();
    }
}
