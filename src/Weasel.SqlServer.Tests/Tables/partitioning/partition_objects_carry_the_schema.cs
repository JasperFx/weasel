using JasperFx;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables.partitioning;

/// <summary>
///     weasel#920. A partition function and scheme are database-scoped, so naming them after the table and
///     column alone made same-named tables in two schemas share them, and a SPLIT or MERGE for one changed
///     the other. New objects carry the schema; a table already on a pre-10.0 name keeps it, because SQL
///     Server cannot rename a partition function and moving a table onto another scheme rewrites its rows.
/// </summary>
[Collection("integration")]
public class partition_objects_carry_the_schema: IntegrationContext
{
    private const string SchemaA = "pfs_a";
    private const string SchemaB = "pfs_b";

    // Names no other test class uses: partition functions are database-scoped, so a name shared with
    // another class would make the two fight over one object
    private const string Readings = "pfs_readings";
    private const string Ledger = "pfs_ledger";
    private const string Orders = "pfs_orders";

    public partition_objects_carry_the_schema(): base(SchemaA)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await theConnection.OpenAsync();
        await resetEverythingAsync();
    }

    // ---------------------------------------------------------------------
    // Naming
    // ---------------------------------------------------------------------

    [Fact]
    public void every_strategy_names_its_objects_after_the_schema_too()
    {
        var table = new Table($"{SchemaA}.{Readings}");

        var range = new RangePartitioning("stamp", "int");
        range.PartitionFunctionName(table).ShouldBe($"pf_{SchemaA}_{Readings}_stamp");
        range.PartitionSchemeName(table).ShouldBe($"ps_{SchemaA}_{Readings}_stamp");

        var rolling = new ManagedRangePartitions(Weasel.Core.Partitioning.RollingWindowPolicy.Monthly(1, 1),
            "occurred_at");
        rolling.PartitionFunctionName(table).ShouldBe($"pf_{SchemaA}_{Readings}_occurred_at");
        rolling.PartitionSchemeName(table).ShouldBe($"ps_{SchemaA}_{Readings}_occurred_at");

        var tenants = new ManagedTenantPartitions("tenants", new DbObjectName(SchemaA, "tenant_registry"));
        tenants.PartitionFunctionName(table).ShouldBe($"pf_{SchemaA}_{Readings}_tenant_ordinal");
        tenants.PartitionSchemeName(table).ShouldBe($"ps_{SchemaA}_{Readings}_tenant_ordinal");

        // The same table name in another schema no longer lands on the same objects
        range.PartitionFunctionName(new Table($"{SchemaB}.{Readings}"))
            .ShouldNotBe(range.PartitionFunctionName(table));
    }

    [Fact]
    public void a_name_too_long_for_sysname_is_shortened_without_colliding()
    {
        var schema = new string('s', 60);
        var first = new Table(new DbObjectName(schema, new string('t', 60) + "_one"));
        var second = new Table(new DbObjectName(schema, new string('t', 60) + "_two"));

        var firstName = SqlServerPartitionObjects.FunctionNameFor(first, "tenant_ordinal");
        var secondName = SqlServerPartitionObjects.FunctionNameFor(second, "tenant_ordinal");

        firstName.Length.ShouldBe(SqlServerPartitionObjects.MaxNameLength);
        secondName.Length.ShouldBe(SqlServerPartitionObjects.MaxNameLength);
        firstName.ShouldNotBe(secondName);

        // Every process has to agree on the name
        SqlServerPartitionObjects.FunctionNameFor(first, "tenant_ordinal").ShouldBe(firstName);
    }

    // ---------------------------------------------------------------------
    // RANGE partitioning, two schemas
    // ---------------------------------------------------------------------

    [Fact]
    public async Task same_named_tables_in_two_schemas_keep_their_own_boundaries()
    {
        await CreateSchemaObjectInDatabase(readings(SchemaA, 10, 20));
        await CreateSchemaObjectInDatabase(readings(SchemaB, 100));

        (await boundariesAsync($"pf_{SchemaA}_{Readings}_stamp")).ShouldBe(["10", "20"]);
        (await boundariesAsync($"pf_{SchemaB}_{Readings}_stamp")).ShouldBe(["100"]);

        (await readings(SchemaA, 10, 20).FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();
        (await readings(SchemaB, 100).FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();

        // SPLIT schema A's function only
        var widened = readings(SchemaA, 10, 20, 30);
        await widened.ApplyChangesAsync(theConnection);

        (await boundariesAsync($"pf_{SchemaA}_{Readings}_stamp")).ShouldBe(["10", "20", "30"]);
        (await boundariesAsync($"pf_{SchemaB}_{Readings}_stamp")).ShouldBe(["100"]);
        (await readings(SchemaB, 100).FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();

        // ...and MERGE it back out again by rolling the split back
        var delta = await widened.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeFalse();

        var split = await readings(SchemaA, 10, 20, 30, 40).FindDeltaAsync(theConnection);
        await runScriptAsync(render(split, update: true));
        (await boundariesAsync($"pf_{SchemaA}_{Readings}_stamp")).ShouldBe(["10", "20", "30", "40"]);

        await runScriptAsync(render(split, update: false));
        (await boundariesAsync($"pf_{SchemaA}_{Readings}_stamp")).ShouldBe(["10", "20", "30"]);
        (await boundariesAsync($"pf_{SchemaB}_{Readings}_stamp")).ShouldBe(["100"]);
    }

    // ---------------------------------------------------------------------
    // A table already on a pre-10.0 name
    // ---------------------------------------------------------------------

    [Fact]
    public async Task a_table_on_a_legacy_named_function_stays_clean_in_the_delta()
    {
        await createLegacyLedgerAsync(SchemaA, 10, 20);

        var expected = ledger(SchemaA, 10, 20);
        var delta = await expected.FindDeltaAsync(theConnection);

        delta.HasChanges().ShouldBeFalse();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        delta.PartitioningDifference.ShouldBe(SchemaPatchDifference.None);

        // Nothing was rebuilt or created alongside it
        (await partitionedOnAsync(SchemaA, Ledger)).ShouldBe($"pf_{Ledger}_stamp");
        (await functionExistsAsync($"pf_{SchemaA}_{Ledger}_stamp")).ShouldBeFalse();
    }

    [Fact]
    public async Task a_new_boundary_on_a_legacy_table_splits_the_function_it_is_on()
    {
        await createLegacyLedgerAsync(SchemaA, 10, 20);
        await insertLedgerRowsAsync(SchemaA);

        var widened = ledger(SchemaA, 10, 20, 30);
        var delta = await widened.FindDeltaAsync(theConnection);
        delta.PartitioningDifference.ShouldBe(SchemaPatchDifference.Update);

        var update = render(delta, update: true);
        update.ShouldContain($"ALTER PARTITION FUNCTION [pf_{Ledger}_stamp]() SPLIT RANGE (30)");
        update.ShouldContain($"ALTER PARTITION SCHEME [ps_{Ledger}_stamp] NEXT USED");
        update.ShouldNotContain($"pf_{SchemaA}_{Ledger}_stamp");

        await widened.ApplyChangesAsync(theConnection);

        (await boundariesAsync($"pf_{Ledger}_stamp")).ShouldBe(["10", "20", "30"]);
        (await partitionedOnAsync(SchemaA, Ledger)).ShouldBe($"pf_{Ledger}_stamp");
        (await widened.FindDeltaAsync(theConnection)).HasChanges().ShouldBeFalse();
        (await countLedgerRowsAsync(SchemaA)).ShouldBe(3);

        // The rollback merges out of the same function
        render(delta, update: false).ShouldContain($"ALTER PARTITION FUNCTION [pf_{Ledger}_stamp]() MERGE RANGE (30)");
    }

    [Fact]
    public async Task a_legacy_function_shared_across_schemas_is_reported()
    {
        await createLegacyLedgerAsync(SchemaA, 10, 20);
        await createLegacyLedgerAsync(SchemaB, 10, 20);

        // A table partitioned from scratch gets objects of its own and is not part of the finding
        await CreateSchemaObjectInDatabase(readings(SchemaA, 10));

        var shared = await SqlServerPartitionObjects.FindSharedAcrossSchemasAsync(theConnection);

        var finding = shared.Single(x => x.FunctionName == $"pf_{Ledger}_stamp");
        finding.SchemeNames.ShouldBe([$"ps_{Ledger}_stamp"]);
        finding.Tables.Select(x => x.QualifiedName).OrderBy(x => x)
            .ShouldBe([$"{SchemaA}.{Ledger}", $"{SchemaB}.{Ledger}"]);

        shared.ShouldNotContain(x => x.FunctionName.Contains(Readings));
    }

    // ---------------------------------------------------------------------
    // ManagedTenantPartitions, two schemas
    // ---------------------------------------------------------------------

    [Fact]
    public async Task managed_tenant_partitions_in_two_schemas_split_and_merge_independently()
    {
        var (managerA, databaseA) = await tenantStoreAsync(SchemaA);
        var (managerB, databaseB) = await tenantStoreAsync(SchemaB);

        await managerA.AddPartitionsToAllTables(NullLoggerFor.Instance, databaseA, ["a1", "a2", "a3"], CancellationToken.None);
        await managerB.AddPartitionsToAllTables(NullLoggerFor.Instance, databaseB, ["b1"], CancellationToken.None);

        (await boundariesAsync($"pf_{SchemaA}_{Orders}_tenant_ordinal")).ShouldBe(["0", "1", "2", "3"]);
        (await boundariesAsync($"pf_{SchemaB}_{Orders}_tenant_ordinal")).ShouldBe(["0", "1"]);

        // Store B dropping its only tenant MERGEs ordinal 1 out of its own function. Before weasel#920 this
        // took ordinal 1 out of store A's partitioning as well.
        await managerB.DropPartitionFromAllTables(NullLoggerFor.Instance, databaseB, ["b1"],
            CancellationToken.None);

        (await boundariesAsync($"pf_{SchemaB}_{Orders}_tenant_ordinal")).ShouldBe(["0"]);
        (await boundariesAsync($"pf_{SchemaA}_{Orders}_tenant_ordinal")).ShouldBe(["0", "1", "2", "3"]);

        // ...and store A's MERGE leaves store B alone
        await managerB.AddPartitionsToAllTables(NullLoggerFor.Instance, databaseB, ["b2", "b3", "b4"],
            CancellationToken.None);
        await managerA.DropPartitionFromAllTables(NullLoggerFor.Instance, databaseA, ["a3"],
            CancellationToken.None);

        (await boundariesAsync($"pf_{SchemaA}_{Orders}_tenant_ordinal")).ShouldBe(["0", "1", "2"]);
        (await boundariesAsync($"pf_{SchemaB}_{Orders}_tenant_ordinal")).ShouldBe(["0", "1", "2", "3"]);
    }

    [Fact]
    public async Task managed_tenant_partitions_split_the_legacy_function_a_table_is_already_on()
    {
        // Two stores from before 10.0: their tables share pf_pfs_orders_tenant_ordinal
        await createLegacyOrdersAsync(SchemaA);
        await createLegacyOrdersAsync(SchemaB);

        var logger = new CapturingLogger();
        var (managerA, databaseA) = await tenantStoreAsync(SchemaA, createTable: false);

        var result = await managerA.AddPartitionsToAllTables(logger, databaseA, ["a1"], CancellationToken.None);
        result.Tables.ShouldAllBe(x => x.Status == PartitionMigrationStatus.Complete);

        // The boundary went into the function the table is on; nothing was created beside it
        (await boundariesAsync($"pf_{Orders}_tenant_ordinal")).ShouldBe(["0", "1"]);
        (await partitionedOnAsync(SchemaA, Orders)).ShouldBe($"pf_{Orders}_tenant_ordinal");
        (await functionExistsAsync($"pf_{SchemaA}_{Orders}_tenant_ordinal")).ShouldBeFalse();

        // ...and the sharing, which still affects store B, is called out
        logger.Warnings.ShouldContain(x => x.Contains($"pf_{Orders}_tenant_ordinal") && x.Contains(SchemaB));
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static Table readings(string schema, params int[] boundaries)
    {
        var table = new Table($"{schema}.{Readings}");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("stamp").AsPrimaryKey();

        var partitioning = table.PartitionByRange("stamp", "int");
        foreach (var boundary in boundaries) partitioning.AddBoundary(boundary);

        return table;
    }

    private static Table ledger(string schema, params int[] boundaries)
    {
        var table = new Table($"{schema}.{Ledger}");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("stamp").AsPrimaryKey();

        var partitioning = table.PartitionByRange("stamp", "int");
        foreach (var boundary in boundaries) partitioning.AddBoundary(boundary);

        return table;
    }

    /// <summary>
    ///     A table exactly as Weasel 9.x and the 10.0 release candidates left it: on a function and scheme
    ///     named after the table and column alone.
    /// </summary>
    private async Task createLegacyLedgerAsync(string schema, params int[] boundaries)
    {
        await runScriptAsync($"""
            IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_{Ledger}_stamp')
                CREATE PARTITION FUNCTION [pf_{Ledger}_stamp] (int) AS RANGE RIGHT FOR VALUES ({string.Join(", ", boundaries)});
            IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_{Ledger}_stamp')
                CREATE PARTITION SCHEME [ps_{Ledger}_stamp] AS PARTITION [pf_{Ledger}_stamp] ALL TO ([PRIMARY]);
            GO
            CREATE TABLE [{schema}].[{Ledger}] (
                [id] int NOT NULL,
                [stamp] int NOT NULL,
                CONSTRAINT [pkey_{Ledger}_id_stamp] PRIMARY KEY CLUSTERED ([id], [stamp])
            ) ON [ps_{Ledger}_stamp]([stamp]);
            """);
    }

    private async Task createLegacyOrdersAsync(string schema)
    {
        await runScriptAsync($"""
            IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_{Orders}_tenant_ordinal')
                CREATE PARTITION FUNCTION [pf_{Orders}_tenant_ordinal] (int) AS RANGE RIGHT FOR VALUES (0);
            IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_{Orders}_tenant_ordinal')
                CREATE PARTITION SCHEME [ps_{Orders}_tenant_ordinal] AS PARTITION [pf_{Orders}_tenant_ordinal] ALL TO ([PRIMARY]);
            GO
            CREATE TABLE [{schema}].[{Orders}] (
                [tenant_ordinal] int NOT NULL,
                [id] int NOT NULL,
                CONSTRAINT [pkey_{Orders}_tenant_ordinal_id] PRIMARY KEY CLUSTERED ([tenant_ordinal], [id])
            ) ON [ps_{Orders}_tenant_ordinal]([tenant_ordinal]);
            """);
    }

    private async Task<(ManagedTenantPartitions, DatabaseWithTables)> tenantStoreAsync(string schema,
        bool createTable = true)
    {
        var manager = new ManagedTenantPartitions("tenants", new DbObjectName(schema, "tenant_registry"));

        var orders = new Table($"{schema}.{Orders}");
        orders.AddColumn<int>("tenant_ordinal").AsPrimaryKey().NotNull();
        orders.AddColumn<int>("id").AsPrimaryKey();
        orders.PartitionByManagedTenants(manager);

        var database = new DatabaseWithTables($"pfs_{schema}", ConnectionSource.ConnectionString);
        database.AddTable(orders);

        if (createTable)
        {
            await CreateSchemaObjectInDatabase(orders);
        }

        await manager.InitializeAsync(theConnection, CancellationToken.None);

        return (manager, database);
    }

    private static string render(TableDelta delta, bool update)
    {
        var writer = new StringWriter();
        if (update)
        {
            delta.WriteUpdate(new SqlServerMigrator(), writer);
        }
        else
        {
            delta.WriteRollback(new SqlServerMigrator(), writer);
        }

        return writer.ToString();
    }

    private async Task insertLedgerRowsAsync(string schema)
    {
        await theConnection.CreateCommand(
                $"insert into [{schema}].[{Ledger}] (id, stamp) values (1, 5), (2, 15), (3, 35)")
            .ExecuteNonQueryAsync();
    }

    private async Task<int> countLedgerRowsAsync(string schema)
        => (int)(await theConnection.CreateCommand($"select count(*) from [{schema}].[{Ledger}]")
            .ExecuteScalarAsync())!;

    private async Task<string[]> boundariesAsync(string functionName)
    {
        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = """
                          SELECT CAST(prv.value AS nvarchar(100))
                          FROM sys.partition_functions pf
                          JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id
                          WHERE pf.name = @pf
                          ORDER BY prv.boundary_id;
                          """;
        cmd.Parameters.AddWithValue("@pf", functionName);

        var list = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(reader.GetString(0));
        }

        return list.ToArray();
    }

    private async Task<bool> functionExistsAsync(string functionName)
    {
        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sys.partition_functions WHERE name = @pf";
        cmd.Parameters.AddWithValue("@pf", functionName);

        return (int)(await cmd.ExecuteScalarAsync())! > 0;
    }

    private async Task<string?> partitionedOnAsync(string schema, string table)
    {
        await using var cmd = theConnection.CreateCommand();
        cmd.CommandText = """
                          SELECT pf.name FROM sys.indexes i
                          JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
                          JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
                          WHERE i.object_id = OBJECT_ID(@table) AND i.index_id <= 1
                          """;
        cmd.Parameters.AddWithValue("@table", $"[{schema}].[{table}]");

        return (string?)await cmd.ExecuteScalarAsync();
    }

    private async Task runScriptAsync(string script)
    {
        foreach (var batch in SqlServerBatchSplitter.Split(script))
        {
            if (string.IsNullOrWhiteSpace(batch)) continue;
            await using var cmd = new SqlCommand(batch, theConnection);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    ///     Both schemas, then every partition function and scheme this class can create: they are
    ///     database-scoped, so dropping the schemas leaves them behind.
    /// </summary>
    private async Task resetEverythingAsync()
    {
        await theConnection.ResetSchemaAsync(SchemaA);
        await theConnection.ResetSchemaAsync(SchemaB);

        var names = new List<(string Scheme, string Function)>
        {
            ($"ps_{Ledger}_stamp", $"pf_{Ledger}_stamp"),
            ($"ps_{Orders}_tenant_ordinal", $"pf_{Orders}_tenant_ordinal")
        };

        foreach (var schema in new[] { SchemaA, SchemaB })
        {
            names.Add(($"ps_{schema}_{Readings}_stamp", $"pf_{schema}_{Readings}_stamp"));
            names.Add(($"ps_{schema}_{Ledger}_stamp", $"pf_{schema}_{Ledger}_stamp"));
            names.Add(($"ps_{schema}_{Orders}_tenant_ordinal", $"pf_{schema}_{Orders}_tenant_ordinal"));
        }

        foreach (var (scheme, function) in names)
        {
            await theConnection.CreateCommand($"""
                IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{scheme}')
                    DROP PARTITION SCHEME [{scheme}];
                IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{function}')
                    DROP PARTITION FUNCTION [{function}];
                """).ExecuteNonQueryAsync();
        }
    }

    private static class NullLoggerFor
    {
        public static readonly ILogger Instance = Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    }

    private sealed class CapturingLogger: ILogger
    {
        public List<string> Warnings { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                Warnings.Add(formatter(state, exception));
            }
        }
    }
}
