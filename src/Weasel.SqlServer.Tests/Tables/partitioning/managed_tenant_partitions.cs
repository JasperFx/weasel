using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables.partitioning;

[Collection("integration")]
public class managed_tenant_partitions: IntegrationContext
{
    public managed_tenant_partitions() : base("mtp")
    {
    }

    // ---------------------------------------------------------------------
    // Pure DDL — no database
    // ---------------------------------------------------------------------

    [Fact]
    public void empty_registry_creates_partition_function_with_sentinel_boundary()
    {
        // SQL Server rejects CREATE PARTITION FUNCTION with no FOR VALUES, so
        // the strategy seeds boundary 0 as a sentinel that real tenant ordinals
        // (allocated from 1+) never collide with.
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));

        var table = new Table("mtp.orders");
        table.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        table.AddColumn<int>("id").AsPrimaryKey();
        table.PartitionByManagedTenants(manager);

        var writer = new StringWriter();
        table.WriteCreateStatement(new SqlServerMigrator(), writer);
        var ddl = writer.ToString();

        ddl.ShouldContain(
            "CREATE PARTITION FUNCTION [pf_orders_tenant_ordinal] (int) AS RANGE RIGHT FOR VALUES (0)");
        ddl.ShouldContain(
            "CREATE PARTITION SCHEME [ps_orders_tenant_ordinal] AS PARTITION [pf_orders_tenant_ordinal] ALL TO ([PRIMARY]);");
        ddl.ShouldContain("ON [ps_orders_tenant_ordinal]([tenant_ordinal])");
    }

    [Fact]
    public void registered_tenants_become_partition_function_boundaries()
    {
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));
        // Reach into the in-memory map via ResetValues-style path. Easier in
        // a unit test to feed ordinals directly than to round-trip through a
        // database.
        SeedOrdinals(manager, ("acme", 1), ("globex", 2), ("initech", 3));

        var table = new Table("mtp.events");
        table.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        table.AddColumn<int>("id").AsPrimaryKey();
        table.PartitionByManagedTenants(manager);

        var writer = new StringWriter();
        table.WriteCreateStatement(new SqlServerMigrator(), writer);
        var ddl = writer.ToString();

        ddl.ShouldContain(
            "CREATE PARTITION FUNCTION [pf_events_tenant_ordinal] (int) AS RANGE RIGHT FOR VALUES (0, 1, 2, 3)");
    }

    [Fact]
    public void boundaries_are_emitted_in_ascending_order_regardless_of_registration_order()
    {
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));
        SeedOrdinals(manager, ("zeta", 3), ("alpha", 1), ("beta", 2));

        var table = new Table("mtp.events");
        table.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        table.AddColumn<int>("id").AsPrimaryKey();
        table.PartitionByManagedTenants(manager);

        var writer = new StringWriter();
        table.WriteCreateStatement(new SqlServerMigrator(), writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("FOR VALUES (0, 1, 2, 3)");
    }

    [Fact]
    public void custom_column_name_is_honored()
    {
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"),
            column: "tenant_slot");

        var table = new Table("mtp.events");
        table.AddColumn<int>("tenant_slot").AsPrimaryKey().NotNull();
        table.AddColumn<int>("id").AsPrimaryKey();
        table.PartitionByManagedTenants(manager);

        var writer = new StringWriter();
        table.WriteCreateStatement(new SqlServerMigrator(), writer);
        var ddl = writer.ToString();

        ddl.ShouldContain("CREATE PARTITION FUNCTION [pf_events_tenant_slot] (int) AS RANGE RIGHT");
        ddl.ShouldContain("ON [ps_events_tenant_slot]([tenant_slot])");
    }

    [Fact]
    public void delta_detects_no_change_when_boundaries_match()
    {
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));
        SeedOrdinals(manager, ("acme", 1), ("globex", 2));

        // Sentinel 0 is always present in the actual database state once the
        // partition function has been provisioned by this strategy.
        var actual = new SqlServerPartitionInfo
        {
            Column = "tenant_ordinal",
            SqlDataType = "int",
            IsRangeRight = true,
            BoundaryValues = ["0", "1", "2"]
        };

        manager.CreateDelta(actual).ShouldBe(PartitionDelta.None);
    }

    [Fact]
    public void delta_detects_additive_when_new_tenants_registered()
    {
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));
        SeedOrdinals(manager, ("a", 1), ("b", 2), ("c", 3), ("d", 4));

        var actual = new SqlServerPartitionInfo
        {
            Column = "tenant_ordinal",
            SqlDataType = "int",
            IsRangeRight = true,
            BoundaryValues = ["0", "1", "2"]
        };

        manager.CreateDelta(actual).ShouldBe(PartitionDelta.Additive);
    }

    [Fact]
    public void delta_detects_rebuild_on_column_drift()
    {
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"),
            column: "tenant_ordinal");
        SeedOrdinals(manager, ("acme", 1));

        var actual = new SqlServerPartitionInfo
        {
            Column = "wrong_column",
            SqlDataType = "int",
            IsRangeRight = true,
            BoundaryValues = ["0", "1"]
        };

        manager.CreateDelta(actual).ShouldBe(PartitionDelta.Rebuild);
    }

    [Fact]
    public void delta_detects_rebuild_when_unknown_boundary_present()
    {
        // A boundary in the database that our in-memory map doesn't know about
        // means the partition function was hand-edited. Don't try to be clever
        // about merging it in — flag for rebuild.
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));
        SeedOrdinals(manager, ("acme", 1));

        var actual = new SqlServerPartitionInfo
        {
            Column = "tenant_ordinal",
            SqlDataType = "int",
            IsRangeRight = true,
            BoundaryValues = ["0", "1", "999"]
        };

        manager.CreateDelta(actual).ShouldBe(PartitionDelta.Rebuild);
    }

    [Fact]
    public void write_split_statements_targets_only_missing_boundaries()
    {
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));
        SeedOrdinals(manager, ("a", 1), ("b", 2), ("c", 3), ("d", 4));

        var table = new Table("mtp.events");
        table.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        table.AddColumn<int>("id").AsPrimaryKey();
        table.PartitionByManagedTenants(manager);

        var actual = new SqlServerPartitionInfo
        {
            Column = "tenant_ordinal",
            SqlDataType = "int",
            IsRangeRight = true,
            BoundaryValues = ["0", "1", "2"]
        };

        var writer = new StringWriter();
        manager.WriteSplitStatements(writer, table, actual);
        var ddl = writer.ToString();

        ddl.ShouldNotContain("SPLIT RANGE (0)");
        ddl.ShouldNotContain("SPLIT RANGE (1)");
        ddl.ShouldNotContain("SPLIT RANGE (2)");
        ddl.ShouldContain("ALTER PARTITION SCHEME [ps_events_tenant_ordinal] NEXT USED [PRIMARY];");
        ddl.ShouldContain("ALTER PARTITION FUNCTION [pf_events_tenant_ordinal]() SPLIT RANGE (3);");
        ddl.ShouldContain("ALTER PARTITION FUNCTION [pf_events_tenant_ordinal]() SPLIT RANGE (4);");
    }

    [Fact]
    public void partition_by_managed_tenants_wires_partitioning_slot()
    {
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));

        var table = new Table("mtp.events");
        table.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        table.AddColumn<int>("id").AsPrimaryKey();
        var ret = table.PartitionByManagedTenants(manager);

        ret.ShouldBeSameAs(manager);
        table.SqlServerPartitioning.ShouldBeSameAs(manager);
    }

    [Fact]
    public void constructor_rejects_blank_column()
    {
        Should.Throw<ArgumentException>(() =>
            new ManagedTenantPartitions("t", new DbObjectName("mtp", "r"), column: ""));
        Should.Throw<ArgumentException>(() =>
            new ManagedTenantPartitions("t", new DbObjectName("mtp", "r"), column: "   "));
    }

    [Fact]
    public void registry_table_is_returned_as_a_schema_object()
    {
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));

        var objects = ((Weasel.Core.Migrations.IFeatureSchema)manager).Objects;
        objects.Length.ShouldBe(1);
        objects[0].ShouldBeOfType<Table>().Identifier.QualifiedName.ShouldBe("mtp.tenant_registry");
    }

    // ---------------------------------------------------------------------
    // Integration — round-trip against the docker SQL Server container
    // ---------------------------------------------------------------------

    /// <summary>
    ///     Partition functions and schemes live at the database level, not the
    ///     schema level, so the inherited <c>ResetSchema()</c> from
    ///     IntegrationContext leaves them dangling between tests. Wipe them
    ///     explicitly so every test starts from a clean slate.
    /// </summary>
    private async Task ResetSchemaAndPartitionObjects()
    {
        await ResetSchema();

        foreach (var pfName in new[] { "pf_orders_tenant_ordinal", "pf_events_tenant_ordinal" })
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

    [Fact]
    public async Task initialize_creates_registry_and_loads_empty_map()
    {
        await ResetSchemaAndPartitionObjects();

        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));

        await manager.InitializeAsync(theConnection, CancellationToken.None);

        manager.Ordinals.ShouldBeEmpty();

        // Registry table should exist
        var exists = await theConnection.CreateCommand(
                "SELECT COUNT(*) FROM sys.objects WHERE object_id = OBJECT_ID(N'mtp.tenant_registry') AND type = 'U'")
            .ExecuteScalarAsync();
        ((int)exists!).ShouldBe(1);
    }

    [Fact]
    public async Task add_partition_to_all_tables_allocates_ordinals_and_splits_partitions()
    {
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtp_integration", ConnectionSource.ConnectionString);
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));

        var orders = new Table("mtp.orders");
        orders.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.PartitionByManagedTenants(manager);
        database.AddTable(orders);

        await CreateSchemaObjectInDatabase(orders);
        await manager.InitializeAsync(theConnection, CancellationToken.None);

        var firstOrdinal = await manager.AddPartitionToAllTables(database, "acme", CancellationToken.None);
        var secondOrdinal = await manager.AddPartitionToAllTables(database, "globex", CancellationToken.None);

        firstOrdinal.ShouldBe(1);
        secondOrdinal.ShouldBe(2);
        manager.Ordinals.ShouldContainKeyAndValue("acme", 1);
        manager.Ordinals.ShouldContainKeyAndValue("globex", 2);

        // Partition function should have boundaries 1 and 2
        var boundaries = await ReadBoundariesAsync("pf_orders_tenant_ordinal");
        boundaries.ShouldBe(new[] { 0, 1, 2 });

        // Registry table should have two rows
        var registryCount = (int)(await theConnection.CreateCommand(
                "SELECT COUNT(*) FROM mtp.tenant_registry")
            .ExecuteScalarAsync())!;
        registryCount.ShouldBe(2);
    }

    [Fact]
    public async Task add_partition_is_idempotent_for_same_tenant()
    {
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtp_integration", ConnectionSource.ConnectionString);
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));

        var orders = new Table("mtp.orders");
        orders.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.PartitionByManagedTenants(manager);
        database.AddTable(orders);

        await CreateSchemaObjectInDatabase(orders);

        var first = await manager.AddPartitionToAllTables(database, "acme", CancellationToken.None);
        var second = await manager.AddPartitionToAllTables(database, "acme", CancellationToken.None);

        first.ShouldBe(1);
        second.ShouldBe(1);

        var boundaries = await ReadBoundariesAsync("pf_orders_tenant_ordinal");
        boundaries.ShouldBe(new[] { 0, 1 });

        var registryCount = (int)(await theConnection.CreateCommand(
                "SELECT COUNT(*) FROM mtp.tenant_registry")
            .ExecuteScalarAsync())!;
        registryCount.ShouldBe(1);
    }

    [Fact]
    public async Task add_partition_splits_multiple_tables_sharing_the_manager()
    {
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtp_integration", ConnectionSource.ConnectionString);
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));

        var orders = new Table("mtp.orders");
        orders.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.PartitionByManagedTenants(manager);

        var events = new Table("mtp.events");
        events.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        events.AddColumn<int>("id").AsPrimaryKey();
        events.PartitionByManagedTenants(manager);

        database.AddTable(orders);
        database.AddTable(events);

        await CreateSchemaObjectInDatabase(orders);
        await CreateSchemaObjectInDatabase(events);

        await manager.AddPartitionToAllTables(database, "acme", CancellationToken.None);
        await manager.AddPartitionToAllTables(database, "globex", CancellationToken.None);

        // Both tables' partition functions should now hold the sentinel 0
        // plus tenant ordinals 1 and 2.
        (await ReadBoundariesAsync("pf_orders_tenant_ordinal"))
            .ShouldBe(new[] { 0, 1, 2 });
        (await ReadBoundariesAsync("pf_events_tenant_ordinal"))
            .ShouldBe(new[] { 0, 1, 2 });
    }

    [Fact]
    public async Task add_partition_after_a_table_is_dropped_recreates_function_via_migrate()
    {
        // Confirms that splitTablesForNewOrdinalsAsync handles the "partition
        // function is missing" branch — i.e. when a table was dropped (or never
        // created) the manager calls MigrateAsync on the table to provision the
        // PF/PS instead of trying to SPLIT a non-existent function.
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtp_integration", ConnectionSource.ConnectionString);
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));

        var orders = new Table("mtp.orders");
        orders.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.PartitionByManagedTenants(manager);
        database.AddTable(orders);

        // Don't pre-create the table; let AddPartitionToAllTables drive
        // provisioning end-to-end.

        await manager.AddPartitionToAllTables(database, "acme", CancellationToken.None);

        var boundaries = await ReadBoundariesAsync("pf_orders_tenant_ordinal");
        boundaries.ShouldBe(new[] { 0, 1 });
    }

    [Fact]
    public async Task drop_partition_from_all_tables_merges_boundaries()
    {
        await ResetSchemaAndPartitionObjects();

        var database = new DatabaseWithTables("mtp_integration", ConnectionSource.ConnectionString);
        var manager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));

        var orders = new Table("mtp.orders");
        orders.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.PartitionByManagedTenants(manager);
        database.AddTable(orders);

        await CreateSchemaObjectInDatabase(orders);

        await manager.AddPartitionToAllTables(database, "acme", CancellationToken.None);
        await manager.AddPartitionToAllTables(database, "globex", CancellationToken.None);
        await manager.AddPartitionToAllTables(database, "initech", CancellationToken.None);

        await manager.DropPartitionFromAllTables(
            NullLogger.Instance, database, new[] { "globex" }, CancellationToken.None);

        manager.Ordinals.ShouldNotContainKey("globex");
        manager.Ordinals.ShouldContainKeyAndValue("acme", 1);
        manager.Ordinals.ShouldContainKeyAndValue("initech", 3);

        // Boundary 2 should have been MERGE'd out; sentinel 0 plus
        // boundaries 1 and 3 remain.
        var boundaries = await ReadBoundariesAsync("pf_orders_tenant_ordinal");
        boundaries.ShouldBe(new[] { 0, 1, 3 });

        var registryCount = (int)(await theConnection.CreateCommand(
                "SELECT COUNT(*) FROM mtp.tenant_registry")
            .ExecuteScalarAsync())!;
        registryCount.ShouldBe(2);
    }

    [Fact]
    public async Task initialize_loads_existing_registry_rows()
    {
        await ResetSchemaAndPartitionObjects();

        var firstManager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));
        var database = new DatabaseWithTables("mtp_integration", ConnectionSource.ConnectionString);

        var orders = new Table("mtp.orders");
        orders.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.PartitionByManagedTenants(firstManager);
        database.AddTable(orders);
        await CreateSchemaObjectInDatabase(orders);

        await firstManager.AddPartitionToAllTables(database, "acme", CancellationToken.None);
        await firstManager.AddPartitionToAllTables(database, "globex", CancellationToken.None);

        // A fresh manager that re-reads the registry should reconstruct the
        // same ordinal map.
        var secondManager = new ManagedTenantPartitions(
            "tenants", new DbObjectName("mtp", "tenant_registry"));
        await secondManager.InitializeAsync(theConnection, CancellationToken.None);

        secondManager.Ordinals.ShouldContainKeyAndValue("acme", 1);
        secondManager.Ordinals.ShouldContainKeyAndValue("globex", 2);
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

    /// <summary>
    ///     Drive the manager's internal tenant -&gt; ordinal map directly for
    ///     pure-DDL/delta tests where round-tripping through the database is
    ///     unnecessary. Mirrors what <c>ResetValues</c> does in-memory.
    /// </summary>
    private static void SeedOrdinals(
        ManagedTenantPartitions manager,
        params (string tenantId, int ordinal)[] entries)
    {
        // The only legal way into _ordinals is through ResetValues (writes DB)
        // or AddPartitionToAllTables (writes DB + DDL). For pure unit tests we
        // synthesize a SqlServerPartitionInfo + CreateDelta round-trip instead
        // — actually simpler: just reflect.
        var field = typeof(ManagedTenantPartitions).GetField(
            "_ordinals",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var map = (Dictionary<string, int>)field!.GetValue(manager)!;
        map.Clear();
        foreach (var (tenantId, ordinal) in entries)
        {
            map[tenantId] = ordinal;
        }
    }
}
