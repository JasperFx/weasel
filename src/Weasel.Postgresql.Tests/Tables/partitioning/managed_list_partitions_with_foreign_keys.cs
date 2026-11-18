using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

/// <summary>
///     weasel#890: dropping a managed partition when the managed tables have foreign keys between them.
///     PostgreSQL refuses to detach a partition while any row still references it, even from a referencing
///     partition that has already been detached, so the referencing tables' partitions have to be detached
///     and dropped first.
/// </summary>
[Collection("managed_list_fks")]
public class managed_list_partitions_with_foreign_keys: IntegrationContext
{
    private const string SchemaName = "managed_list_fks";

    public managed_list_partitions_with_foreign_keys() : base(SchemaName)
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    private static Table tenantTable(string name)
    {
        var table = new Table(new DbObjectName(SchemaName, name));
        table.AddColumn<string>("tenant_id").AsPrimaryKey().NotNull();
        table.AddColumn<int>("id").AsPrimaryKey();
        return table;
    }

    private static void addForeignKey(Table table, string column, Table referenced)
    {
        table.AddColumn<int>(column);
        table.ForeignKeys.Add(new ForeignKey($"fk_{table.Identifier.Name}_{column}")
        {
            ColumnNames = ["tenant_id", column],
            LinkedNames = ["tenant_id", "id"],
            LinkedTable = referenced.Identifier
        });
    }

    /// <summary>
    ///     The tables are registered referenced-first, the order a feature schema yields them in because
    ///     they have to be created in it. That is the order that used to break the drop.
    /// </summary>
    private static async Task<ForeignKeyDatabase> theDatabase(params Table[] tables)
    {
        var database = new ForeignKeyDatabase(tables);
        await database.Partitions.ResetValues(database,
            new Dictionary<string, string> { { "red", "red" }, { "blue", "blue" } }, CancellationToken.None);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();
        return database;
    }

    private async Task execute(string sql)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await conn.CreateCommand(sql).ExecuteNonQueryAsync();
    }

    private static async Task<string[]> tenantsIn(string tableName)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var reader = await conn
            .CreateCommand($"select tenant_id from {SchemaName}.{tableName} order by tenant_id, id")
            .ExecuteReaderAsync();

        var list = new List<string>();
        while (await reader.ReadAsync())
        {
            list.Add(reader.GetString(0));
        }

        return list.ToArray();
    }

    private static async Task<bool> tableExists(string name)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        var count = (long)(await conn.CreateCommand(
                "select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = :schema and c.relname = :name")
            .With("schema", SchemaName)
            .With("name", name)
            .ExecuteScalarAsync())!;
        return count > 0;
    }

    [Fact]
    public async Task dropping_a_partition_drops_the_referencing_and_the_referenced_partition()
    {
        var customers = tenantTable("customers");
        var orders = tenantTable("orders");
        addForeignKey(orders, "customer_id", customers);

        var database = await theDatabase(customers, orders);

        await execute($"insert into {SchemaName}.customers (tenant_id, id) values ('red', 1), ('blue', 1)");
        await execute(
            $"insert into {SchemaName}.orders (tenant_id, id, customer_id) values ('red', 10, 1), ('red', 11, 1), ('blue', 10, 1)");

        await database.Partitions.DropPartitionFromAllTables(database, NullLogger.Instance, ["red"],
            CancellationToken.None);

        (await tableExists("orders_red")).ShouldBeFalse();
        (await tableExists("customers_red")).ShouldBeFalse();

        (await tenantsIn("orders")).ShouldBe(["blue"]);
        (await tenantsIn("customers")).ShouldBe(["blue"]);

        (await tableExists("orders_blue")).ShouldBeTrue();
        (await tableExists("customers_blue")).ShouldBeTrue();
    }

    [Fact]
    public async Task dropping_by_value_drops_the_referencing_and_the_referenced_partition()
    {
        var customers = tenantTable("customers");
        var orders = tenantTable("orders");
        addForeignKey(orders, "customer_id", customers);

        var database = await theDatabase(customers, orders);

        await execute($"insert into {SchemaName}.customers (tenant_id, id) values ('red', 1), ('blue', 1)");
        await execute($"insert into {SchemaName}.orders (tenant_id, id, customer_id) values ('red', 10, 1), ('blue', 10, 1)");

        await database.Partitions.DropPartitionFromAllTablesForValue(database, NullLogger.Instance, "red",
            CancellationToken.None);

        (await tableExists("orders_red")).ShouldBeFalse();
        (await tableExists("customers_red")).ShouldBeFalse();

        (await tenantsIn("orders")).ShouldBe(["blue"]);
        (await tenantsIn("customers")).ShouldBe(["blue"]);
    }

    [Fact]
    public async Task dropping_a_partition_of_a_self_referencing_table()
    {
        var nodes = tenantTable("nodes");
        addForeignKey(nodes, "parent_id", nodes);

        var customers = tenantTable("customers");
        var orders = tenantTable("orders");
        addForeignKey(orders, "customer_id", customers);

        var database = await theDatabase(nodes, customers, orders);

        // A small tree per tenant: 1 <- 2 <- 3
        await execute(
            $"insert into {SchemaName}.nodes (tenant_id, id, parent_id) values ('red', 1, null), ('red', 2, 1), ('red', 3, 2), ('blue', 1, null), ('blue', 2, 1)");
        await execute($"insert into {SchemaName}.customers (tenant_id, id) values ('red', 1), ('blue', 1)");
        await execute($"insert into {SchemaName}.orders (tenant_id, id, customer_id) values ('red', 10, 1), ('blue', 10, 1)");

        await database.Partitions.DropPartitionFromAllTables(database, NullLogger.Instance, ["red"],
            CancellationToken.None);

        (await tableExists("nodes_red")).ShouldBeFalse();
        (await tableExists("orders_red")).ShouldBeFalse();
        (await tableExists("customers_red")).ShouldBeFalse();

        (await tenantsIn("nodes")).ShouldBe(["blue", "blue"]);
        (await tenantsIn("orders")).ShouldBe(["blue"]);
        (await tenantsIn("customers")).ShouldBe(["blue"]);
    }

    [Fact]
    public async Task dropping_a_partition_of_a_table_with_no_foreign_keys_alongside()
    {
        var customers = tenantTable("customers");
        var audit = tenantTable("audit");
        var orders = tenantTable("orders");
        addForeignKey(orders, "customer_id", customers);

        var database = await theDatabase(customers, audit, orders);

        await execute($"insert into {SchemaName}.customers (tenant_id, id) values ('red', 1), ('blue', 1)");
        await execute($"insert into {SchemaName}.audit (tenant_id, id) values ('red', 1), ('red', 2), ('blue', 1)");
        await execute($"insert into {SchemaName}.orders (tenant_id, id, customer_id) values ('red', 10, 1), ('blue', 10, 1)");

        await database.Partitions.DropPartitionFromAllTables(database, NullLogger.Instance, ["red"],
            CancellationToken.None);

        (await tableExists("audit_red")).ShouldBeFalse();
        (await tableExists("orders_red")).ShouldBeFalse();
        (await tableExists("customers_red")).ShouldBeFalse();

        (await tenantsIn("audit")).ShouldBe(["blue"]);
        (await tenantsIn("orders")).ShouldBe(["blue"]);
        (await tenantsIn("customers")).ShouldBe(["blue"]);
    }
}

public class ForeignKeyDatabase: PostgresqlDatabase
{
    public readonly ManagedListPartitions Partitions = new("Partitions",
        new DbObjectName("managed_list_fks", "partitions"));

    private readonly ForeignKeyTables _feature;

    public ForeignKeyDatabase(Table[] tables) : base(new DefaultMigrationLogger(), JasperFx.AutoCreate.CreateOrUpdate,
        new PostgresqlMigrator(), "Partitions", NpgsqlDataSource.Create(ConnectionSource.ConnectionString))
    {
        foreach (var table in tables)
        {
            table.PartitionByList("tenant_id").UsePartitionManager(Partitions);
        }

        _feature = new ForeignKeyTables(tables);
    }

    public override IFeatureSchema[] BuildFeatureSchemas()
    {
        return [_feature];
    }
}

public class ForeignKeyTables: FeatureSchemaBase
{
    private readonly Table[] _tables;

    public ForeignKeyTables(Table[] tables) : base("ForeignKeyTables", new PostgresqlMigrator())
    {
        _tables = tables;
    }

    protected override IEnumerable<ISchemaObject> schemaObjects() => _tables;
}
