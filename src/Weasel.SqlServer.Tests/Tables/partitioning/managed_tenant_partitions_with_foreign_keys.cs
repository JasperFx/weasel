using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables.partitioning;

/// <summary>
///     weasel#890: removing a tenant with <see cref="TenantDropBehavior.DeleteData" /> when the managed
///     tables have foreign keys between them. The purge has to empty the referencing tables first, or the
///     referenced table's DELETE is refused and its rows and boundary are left behind.
/// </summary>
[Collection("integration")]
public class managed_tenant_partitions_with_foreign_keys: IntegrationContext
{
    private static readonly string[] theTableNames = ["fkp_customers", "fkp_orders", "fkp_nodes", "fkp_audit"];

    public managed_tenant_partitions_with_foreign_keys() : base("mtpfk")
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await ResetSchema();

        // Partition functions and schemes live at the database level, so they outlive the schema reset
        foreach (var tableName in theTableNames)
        {
            await theConnection.CreateCommand($@"
IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_mtpfk_{tableName}_tenant_ordinal')
    DROP PARTITION SCHEME [ps_mtpfk_{tableName}_tenant_ordinal];
IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_mtpfk_{tableName}_tenant_ordinal')
    DROP PARTITION FUNCTION [pf_mtpfk_{tableName}_tenant_ordinal];")
                .ExecuteNonQueryAsync();
        }
    }

    private static ManagedTenantPartitions theManager() =>
        new("tenants", new DbObjectName("mtpfk", "tenant_registry"));

    private static Table tenantTable(ManagedTenantPartitions manager, string name)
    {
        var table = new Table($"mtpfk.{name}");
        table.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        table.AddColumn<int>("id").AsPrimaryKey();
        table.PartitionByManagedTenants(manager);
        return table;
    }

    private static void addForeignKey(Table table, string column, Table referenced)
    {
        table.AddColumn<int>(column);
        var fk = table.FindOrCreateForeignKey($"fk_{table.Identifier.Name}_{column}");
        fk.ColumnNames = ["tenant_ordinal", column];
        fk.LinkedNames = ["tenant_ordinal", "id"];
        fk.LinkedTable = referenced.Identifier;
    }

    /// <summary>
    ///     Register the tables the way a feature schema yields them: referenced tables first, since they
    ///     have to be created first. That is the order that used to break the purge.
    /// </summary>
    private async Task<DatabaseWithTables> theDatabase(params Table[] tables)
    {
        var database = new DatabaseWithTables("mtpfk_integration", ConnectionSource.ConnectionString);
        foreach (var table in tables)
        {
            database.AddTable(table);
            await CreateSchemaObjectInDatabase(table);
        }

        return database;
    }

    private async Task execute(string sql)
    {
        await theConnection.CreateCommand(sql).ExecuteNonQueryAsync();
    }

    private async Task<int[]> ordinalsIn(string tableName)
    {
        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = $"SELECT tenant_ordinal FROM mtpfk.{tableName} ORDER BY tenant_ordinal";

        var list = new List<int>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(reader.GetInt32(0));
        }

        return list.ToArray();
    }

    private async Task<int[]> boundariesOf(string tableName)
    {
        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = @"
SELECT CAST(prv.value AS int)
FROM sys.partition_functions pf
JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id
WHERE pf.name = @pf
ORDER BY prv.boundary_id;";
        cmd.Parameters.AddWithValue("@pf", $"pf_mtpfk_{tableName}_tenant_ordinal");

        var list = new List<int>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(reader.GetInt32(0));
        }

        return list.ToArray();
    }

    private static Task dropAcme(ManagedTenantPartitions manager, DatabaseWithTables database) =>
        manager.DropPartitionFromAllTables(NullLogger.Instance, database, ["acme"], TenantDropBehavior.DeleteData,
            CancellationToken.None);

    [Fact]
    public async Task delete_data_empties_the_referencing_and_the_referenced_table()
    {
        var manager = theManager();
        var customers = tenantTable(manager, "fkp_customers");
        var orders = tenantTable(manager, "fkp_orders");
        addForeignKey(orders, "customer_id", customers);

        var database = await theDatabase(customers, orders);

        (await manager.AddPartitionToAllTables(database, "acme", CancellationToken.None)).ShouldBe(1);
        (await manager.AddPartitionToAllTables(database, "globex", CancellationToken.None)).ShouldBe(2);

        await execute("INSERT INTO mtpfk.fkp_customers (tenant_ordinal, id) VALUES (1, 1), (2, 1)");
        await execute(
            "INSERT INTO mtpfk.fkp_orders (tenant_ordinal, id, customer_id) VALUES (1, 10, 1), (1, 11, 1), (2, 10, 1)");

        await dropAcme(manager, database);

        manager.Ordinals.ShouldNotContainKey("acme");

        // Both tables lost acme's rows and its partition; globex is untouched
        (await ordinalsIn("fkp_orders")).ShouldBe([2]);
        (await ordinalsIn("fkp_customers")).ShouldBe([2]);
        (await boundariesOf("fkp_orders")).ShouldBe([0, 2]);
        (await boundariesOf("fkp_customers")).ShouldBe([0, 2]);
    }

    [Fact]
    public async Task delete_data_handles_a_self_referencing_table()
    {
        var manager = theManager();
        var nodes = tenantTable(manager, "fkp_nodes");
        addForeignKey(nodes, "parent_id", nodes);

        var customers = tenantTable(manager, "fkp_customers");
        var orders = tenantTable(manager, "fkp_orders");
        addForeignKey(orders, "customer_id", customers);

        var database = await theDatabase(nodes, customers, orders);

        await manager.AddPartitionToAllTables(database, "acme", CancellationToken.None);
        await manager.AddPartitionToAllTables(database, "globex", CancellationToken.None);

        // A small tree per tenant: 1 <- 2 <- 3
        await execute(
            "INSERT INTO mtpfk.fkp_nodes (tenant_ordinal, id, parent_id) VALUES (1, 1, NULL), (1, 2, 1), (1, 3, 2), (2, 1, NULL), (2, 2, 1)");
        await execute("INSERT INTO mtpfk.fkp_customers (tenant_ordinal, id) VALUES (1, 1), (2, 1)");
        await execute("INSERT INTO mtpfk.fkp_orders (tenant_ordinal, id, customer_id) VALUES (1, 10, 1), (2, 10, 1)");

        await dropAcme(manager, database);

        (await ordinalsIn("fkp_nodes")).ShouldBe([2, 2]);
        (await ordinalsIn("fkp_orders")).ShouldBe([2]);
        (await ordinalsIn("fkp_customers")).ShouldBe([2]);

        (await boundariesOf("fkp_nodes")).ShouldBe([0, 2]);
        (await boundariesOf("fkp_orders")).ShouldBe([0, 2]);
        (await boundariesOf("fkp_customers")).ShouldBe([0, 2]);
    }

    [Fact]
    public async Task delete_data_still_empties_a_table_with_no_foreign_keys()
    {
        var manager = theManager();
        var customers = tenantTable(manager, "fkp_customers");
        var audit = tenantTable(manager, "fkp_audit");
        var orders = tenantTable(manager, "fkp_orders");
        addForeignKey(orders, "customer_id", customers);

        var database = await theDatabase(customers, audit, orders);

        await manager.AddPartitionToAllTables(database, "acme", CancellationToken.None);
        await manager.AddPartitionToAllTables(database, "globex", CancellationToken.None);

        await execute("INSERT INTO mtpfk.fkp_customers (tenant_ordinal, id) VALUES (1, 1), (2, 1)");
        await execute("INSERT INTO mtpfk.fkp_audit (tenant_ordinal, id) VALUES (1, 1), (1, 2), (2, 1)");
        await execute("INSERT INTO mtpfk.fkp_orders (tenant_ordinal, id, customer_id) VALUES (1, 10, 1), (2, 10, 1)");

        await dropAcme(manager, database);

        (await ordinalsIn("fkp_audit")).ShouldBe([2]);
        (await ordinalsIn("fkp_orders")).ShouldBe([2]);
        (await ordinalsIn("fkp_customers")).ShouldBe([2]);

        (await boundariesOf("fkp_audit")).ShouldBe([0, 2]);
        (await boundariesOf("fkp_orders")).ShouldBe([0, 2]);
        (await boundariesOf("fkp_customers")).ShouldBe([0, 2]);
    }
}
