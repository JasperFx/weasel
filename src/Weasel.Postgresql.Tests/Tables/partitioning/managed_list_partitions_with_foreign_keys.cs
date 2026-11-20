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
    private static Task<ForeignKeyDatabase> theDatabase(params Table[] tables)
    {
        return theDatabase(new Dictionary<string, string> { { "red", "red" }, { "blue", "blue" } }, tables);
    }

    private static async Task<ForeignKeyDatabase> theDatabase(Dictionary<string, string> values, params Table[] tables)
    {
        var database = new ForeignKeyDatabase(tables);
        await database.Partitions.ResetValues(database, values, CancellationToken.None);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();
        return database;
    }

    /// <summary>
    ///     The values a partition's bound covers, sorted.
    /// </summary>
    private static async Task<string[]> partitionValues(string name)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        var bound = await conn.CreateCommand(
                "select pg_get_expr(c.relpartbound, c.oid) from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = :schema and c.relname = :name")
            .With("schema", SchemaName)
            .With("name", name)
            .ExecuteScalarAsync() as string;

        if (bound == null) return [];

        var inner = bound.Substring(bound.IndexOf('(') + 1, bound.LastIndexOf(')') - bound.IndexOf('(') - 1);
        return inner.Split(',').Select(x => x.Trim().Trim('\'')).OrderBy(x => x).ToArray();
    }

    private static async Task<int> foreignKeyCount()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        return (int)(long)(await conn.CreateCommand(
                "select count(*) from pg_constraint where contype = 'f' and connamespace = to_regnamespace(:schema) and conparentid = 0")
            .With("schema", SchemaName)
            .ExecuteScalarAsync())!;
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
    public async Task dropping_a_partition_of_a_self_referencing_table_keeps_the_foreign_key()
    {
        // weasel#895: on PostgreSQL 17 the detach of a partition whose rows reference each other is refused, and
        // dropping the attached partition with CASCADE would take the foreign key off the parent table
        var nodes = tenantTable("nodes");
        addForeignKey(nodes, "parent_id", nodes);

        var database = await theDatabase(nodes);

        await execute(
            $"insert into {SchemaName}.nodes (tenant_id, id, parent_id) values ('red', 1, null), ('red', 2, 1), ('red', 3, 2), ('blue', 1, null), ('blue', 2, 1)");

        var foreignKeys = await foreignKeyCount();

        await database.Partitions.DropPartitionFromAllTablesForValue(database, NullLogger.Instance, "red",
            CancellationToken.None);

        (await tableExists("nodes_red")).ShouldBeFalse();
        (await tenantsIn("nodes")).ShouldBe(["blue", "blue"]);

        (await foreignKeyCount()).ShouldBe(foreignKeys);

        // The foreign key is still enforced on the surviving partition
        await Should.ThrowAsync<PostgresException>(() =>
            execute($"insert into {SchemaName}.nodes (tenant_id, id, parent_id) values ('blue', 3, 99)"));
    }

    [Fact]
    public async Task removing_one_value_from_a_shared_partition_of_a_referenced_table()
    {
        // weasel#891: the referenced table's partition can't be detached while the surviving members' rows are
        // referenced, so it keeps its bound; the referencing table is narrowed as before
        var customers = tenantTable("customers");
        var orders = tenantTable("orders");
        addForeignKey(orders, "customer_id", customers);

        var database = await theDatabase(new Dictionary<string, string> { { "a", "small" }, { "b", "small" } },
            customers, orders);

        await execute($"insert into {SchemaName}.customers (tenant_id, id) values ('a', 1), ('b', 1)");
        await execute($"insert into {SchemaName}.orders (tenant_id, id, customer_id) values ('a', 10, 1), ('b', 10, 1)");

        await database.Partitions.DropPartitionFromAllTablesForValue(database, NullLogger.Instance, "a",
            CancellationToken.None);

        (await tenantsIn("orders")).ShouldBe(["b"]);
        (await tenantsIn("customers")).ShouldBe(["b"]);

        (await partitionValues("orders_small")).ShouldBe(["b"]);
        (await partitionValues("customers_small")).ShouldBe(["a", "b"]);

        database.Partitions.Partitions.Keys.ShouldBe(["b"]);
        database.Partitions.ForceReload();
        await database.Partitions.InitializeAsync(database, CancellationToken.None);
        database.Partitions.Partitions.Keys.ShouldBe(["b"]);

        // The survivor still works
        await execute($"insert into {SchemaName}.customers (tenant_id, id) values ('b', 2)");
        await execute($"insert into {SchemaName}.orders (tenant_id, id, customer_id) values ('b', 11, 2)");

        // The departed value can come back into the same bucket
        await database.Partitions.AddPartitionToAllTables(NullLogger.Instance, database,
            new Dictionary<string, string> { { "a", "small" } }, CancellationToken.None);
        (await partitionValues("orders_small")).ShouldBe(["a", "b"]);
        (await partitionValues("customers_small")).ShouldBe(["a", "b"]);
        await execute($"insert into {SchemaName}.customers (tenant_id, id) values ('a', 1)");
        await execute($"insert into {SchemaName}.orders (tenant_id, id, customer_id) values ('a', 10, 1)");

        // and the partition goes with its last member
        await database.Partitions.DropPartitionFromAllTablesForValue(database, NullLogger.Instance, "a",
            CancellationToken.None);
        await database.Partitions.DropPartitionFromAllTablesForValue(database, NullLogger.Instance, "b",
            CancellationToken.None);

        (await tableExists("orders_small")).ShouldBeFalse();
        (await tableExists("customers_small")).ShouldBeFalse();
        database.Partitions.Partitions.ShouldBeEmpty();
    }

    /// <summary>
    ///     The registry's rows, value => suffix
    /// </summary>
    private static async Task<Dictionary<string, string>> registry()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var reader = await conn
            .CreateCommand($"select partition_value, partition_suffix from {SchemaName}.partitions")
            .ExecuteReaderAsync();

        var values = new Dictionary<string, string>();
        while (await reader.ReadAsync())
        {
            values[reader.GetString(0)] = reader.GetString(1);
        }

        return values;
    }

    /// <summary>
    ///     weasel#896: customers (referenced) and orders (referencing customers) share bucket "small" between "a" and
    ///     "b", and "a" has just left it, so customers_small still covers "a" while orders_small doesn't
    /// </summary>
    private async Task<ForeignKeyDatabase> aValueThatLeftASharedBucketOfAReferencedTable()
    {
        var customers = tenantTable("customers");
        var orders = tenantTable("orders");
        addForeignKey(orders, "customer_id", customers);

        var database = await theDatabase(new Dictionary<string, string> { { "a", "small" }, { "b", "small" } },
            customers, orders);

        await execute($"insert into {SchemaName}.customers (tenant_id, id) values ('a', 1), ('b', 1)");
        await execute($"insert into {SchemaName}.orders (tenant_id, id, customer_id) values ('a', 10, 1), ('b', 10, 1)");

        await database.Partitions.DropPartitionFromAllTablesForValue(database, NullLogger.Instance, "a",
            CancellationToken.None);

        (await partitionValues("customers_small")).ShouldBe(["a", "b"]);
        (await partitionValues("orders_small")).ShouldBe(["b"]);

        return database;
    }

    private async Task theValueIsBackInTheSharedBucket(ForeignKeyDatabase database)
    {
        (await tableExists("customers_a")).ShouldBeFalse();
        (await tableExists("orders_a")).ShouldBeFalse();

        (await partitionValues("customers_small")).ShouldBe(["a", "b"]);
        (await partitionValues("orders_small")).ShouldBe(["a", "b"]);

        // The registry and the in-memory map agree with the tables
        (await registry()).ShouldBe(new Dictionary<string, string> { { "a", "small" }, { "b", "small" } },
            ignoreOrder: true);
        database.Partitions.Partitions["a"].ShouldBe("small");
        ((IListPartitionManager)database.Partitions).Partitions().Single().Suffix.ShouldBe("small");

        database.Partitions.ForceReload();
        await database.Partitions.InitializeAsync(database, CancellationToken.None);
        database.Partitions.Partitions["a"].ShouldBe("small");

        // and the value works again on both tables
        await execute($"insert into {SchemaName}.customers (tenant_id, id) values ('a', 1)");
        await execute($"insert into {SchemaName}.orders (tenant_id, id, customer_id) values ('a', 10, 1)");
        (await tenantsIn("orders")).ShouldBe(["a", "b"]);
    }

    [Fact]
    public async Task re_adding_a_value_under_a_different_suffix_goes_back_into_the_bucket_that_still_covers_it()
    {
        // weasel#896: the request for suffix "a" used to create orders_a and fail on customers, whose
        // customers_small bound still covers "a", leaving the two tables disagreeing
        var database = await aValueThatLeftASharedBucketOfAReferencedTable();

        var statuses = await database.Partitions.AddPartitionToAllTables(NullLogger.Instance, database,
            new Dictionary<string, string> { { "a", "a" } }, CancellationToken.None);

        statuses.ShouldAllBe(x => x.Status == PartitionMigrationStatus.Complete);

        await theValueIsBackInTheSharedBucket(database);
    }

    [Fact]
    public async Task re_adding_a_single_value_under_a_different_suffix_goes_back_into_the_bucket_that_still_covers_it()
    {
        var database = await aValueThatLeftASharedBucketOfAReferencedTable();

        await database.Partitions.AddPartitionToAllTables(database, "a", "a", CancellationToken.None);

        await theValueIsBackInTheSharedBucket(database);
    }

    [Fact]
    public async Task resetting_values_keeps_a_covered_value_in_its_bucket()
    {
        var database = await aValueThatLeftASharedBucketOfAReferencedTable();

        await database.Partitions.ResetValues(database,
            new Dictionary<string, string> { { "a", "a" }, { "b", "small" } }, CancellationToken.None);

        (await registry()).ShouldBe(new Dictionary<string, string> { { "a", "small" }, { "b", "small" } },
            ignoreOrder: true);
        database.Partitions.Partitions["a"].ShouldBe("small");
    }

    [Fact]
    public async Task a_value_never_in_a_bucket_still_gets_its_own_partition()
    {
        var database = await aValueThatLeftASharedBucketOfAReferencedTable();

        var statuses = await database.Partitions.AddPartitionToAllTables(NullLogger.Instance, database,
            new Dictionary<string, string> { { "c", "c" } }, CancellationToken.None);

        statuses.ShouldAllBe(x => x.Status == PartitionMigrationStatus.Complete);

        (await partitionValues("customers_c")).ShouldBe(["c"]);
        (await partitionValues("orders_c")).ShouldBe(["c"]);
        (await partitionValues("customers_small")).ShouldBe(["a", "b"]);
        (await partitionValues("orders_small")).ShouldBe(["b"]);

        (await registry()).ShouldBe(new Dictionary<string, string> { { "b", "small" }, { "c", "c" } },
            ignoreOrder: true);
        database.Partitions.Partitions["c"].ShouldBe("c");
    }

    [Fact]
    public async Task re_adding_is_refused_when_a_referenced_table_would_have_to_widen_its_bucket()
    {
        // The other way around from #891: here the referenced table's bucket no longer covers "a" (it was narrowed
        // before the foreign key existed), while the referencing table's still does. Putting "a" back into "small"
        // would need a DETACH of customers_small, which PostgreSQL refuses while b's rows are referenced.
        var customers = tenantTable("customers");
        var orders = tenantTable("orders");
        addForeignKey(orders, "customer_id", customers);

        var database = await theDatabase(new Dictionary<string, string> { { "a", "small" }, { "b", "small" } },
            customers, orders);

        await execute(
            $"alter table {SchemaName}.customers detach partition {SchemaName}.customers_small; alter table {SchemaName}.customers attach partition {SchemaName}.customers_small for values in ('b')");
        await execute($"delete from {SchemaName}.partitions where partition_value = 'a'");
        database.Partitions.ForceReload();

        await execute($"insert into {SchemaName}.customers (tenant_id, id) values ('b', 1)");
        await execute($"insert into {SchemaName}.orders (tenant_id, id, customer_id) values ('b', 10, 1)");

        var ex = await Should.ThrowAsync<InvalidOperationException>(() =>
            database.Partitions.AddPartitionToAllTables(NullLogger.Instance, database,
                new Dictionary<string, string> { { "a", "a" } }, CancellationToken.None));

        ex.Message.ShouldContain("'a'");
        ex.Message.ShouldContain("orders_small");
        ex.Message.ShouldContain("customers_small");

        // Nothing was changed
        (await tableExists("customers_a")).ShouldBeFalse();
        (await tableExists("orders_a")).ShouldBeFalse();
        (await partitionValues("customers_small")).ShouldBe(["b"]);
        (await partitionValues("orders_small")).ShouldBe(["a", "b"]);
        (await registry()).ShouldBe(new Dictionary<string, string> { { "b", "small" } });
        database.Partitions.Partitions.Keys.ShouldBe(["b"]);
    }

    [Fact]
    public async Task removing_one_value_from_a_shared_partition_of_a_self_referencing_table()
    {
        var nodes = tenantTable("nodes");
        addForeignKey(nodes, "parent_id", nodes);
        var audit = tenantTable("audit");

        var database = await theDatabase(new Dictionary<string, string> { { "a", "small" }, { "b", "small" } },
            nodes, audit);

        await execute(
            $"insert into {SchemaName}.nodes (tenant_id, id, parent_id) values ('a', 1, null), ('a', 2, 1), ('b', 1, null), ('b', 2, 1)");
        await execute($"insert into {SchemaName}.audit (tenant_id, id) values ('a', 1), ('b', 1)");

        await database.Partitions.DropPartitionFromAllTablesForValue(database, NullLogger.Instance, "a",
            CancellationToken.None);

        (await tenantsIn("nodes")).ShouldBe(["b", "b"]);
        (await tenantsIn("audit")).ShouldBe(["b"]);

        (await partitionValues("nodes_small")).ShouldBe(["a", "b"]);

        // A table nothing references is still narrowed
        (await partitionValues("audit_small")).ShouldBe(["b"]);
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
