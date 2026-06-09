using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;
using Shouldly;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

[Collection("managed_lists")]
public class managed_list_partitions : IntegrationContext
{
    public managed_list_partitions() : base("managed_lists")
    {

    }

    [Fact]
    public async Task can_load_values_smoke_test()
    {
        var database = new ManagedListDatabase();
        var partitions = new Dictionary<string, string> { { "red", "red" }, { "green", "green" }, { "blue", "blue" }, };
        await database.Partitions.ResetValues(database, partitions, CancellationToken.None);
    }

    [Fact]
    public async Task initialize_smoke_test()
    {
        var database = new ManagedListDatabase();
        var partitions = new Dictionary<string, string> { { "red", "red" }, { "green", "green" }, { "blue", "blue" }, };
        await database.Partitions.ResetValues(database, partitions, CancellationToken.None);

        await database.Partitions.InitializeAsync(database, CancellationToken.None);

        foreach (var partition in partitions)
        {
            database.Partitions.Partitions[partition.Key].ShouldBe(partition.Value);
        }
    }

    [Fact]
    public async Task migrate_tables_smoke_test()
    {
        var database = new ManagedListDatabase();
        var partitions = new Dictionary<string, string> { { "red", "red" }, { "green", "green" }, { "blue", "blue" }, };
        await database.Partitions.ResetValues(database, partitions, CancellationToken.None);

        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var tables = await database.FetchExistingTablesAsync();

        var teams = tables.Single(x => x.Identifier.Name == "teams");
        var partitioning = teams.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.HasExistingDefault.ShouldBeFalse();
        partitioning.Partitions.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(new []{"blue", "green", "red"});

        var players = tables.Single(x => x.Identifier.Name == "players");
        partitioning = players.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.HasExistingDefault.ShouldBeFalse();
        partitioning.Partitions.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(new []{"blue", "green", "red"});

    }

    [Fact]
    public async Task managed_partition_with_default_suffix_is_idempotent()
    {
        // Mirrors a managed partition whose suffix is "default" — e.g. Marten's *DEFAULT* tenant,
        // registered as partition value "*DEFAULT*" with table suffix "default". Its partition table
        // is named "<table>_default", which collided with the conventional PostgreSQL DEFAULT-partition
        // name. The diff then perpetually reported it missing and re-issued CREATE TABLE ..._default,
        // failing the SECOND migration with 42P07 "relation already exists". A regular value partition
        // must be recognized by its bound expression, not its name.
        var database = new ManagedListDatabase();
        var partitions = new Dictionary<string, string> { { "tenant1", "tenant1" }, { "*DEFAULT*", "default" } };
        await database.Partitions.ResetValues(database, partitions, CancellationToken.None);

        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // The "<table>_default" partition is a value partition (FOR VALUES IN ('*DEFAULT*')), not the
        // PostgreSQL DEFAULT partition.
        var tables = await database.FetchExistingTablesAsync();
        var teams = tables.Single(x => x.Identifier.Name == "teams");
        var partitioning = teams.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.HasExistingDefault.ShouldBeFalse();
        partitioning.Partitions.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(new []{"default", "tenant1"});

        // No pending delta, and a second migration must NOT throw 42P07.
        await database.AssertDatabaseMatchesConfigurationAsync();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    [Fact]
    public async Task migrate_tables_smoke_test_with_variable_value_and_tenant_id()
    {
        var database = new ManagedListDatabase();
        var partitions = new Dictionary<string, string> { { Guid.NewGuid().ToString(), "red" }, { Guid.NewGuid().ToString(), "green" }, { Guid.NewGuid().ToString(), "blue" }, };
        //await database.Partitions.ResetValues(database, partitions, CancellationToken.None);

        await database.Partitions.AddPartitionToAllTables(NullLogger.Instance, database, partitions, CancellationToken.None);

        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var tables = await database.FetchExistingTablesAsync();

        var teams = tables.Single(x => x.Identifier.Name == "teams");
        var partitioning = teams.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.HasExistingDefault.ShouldBeFalse();
        partitioning.Partitions.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(new []{"blue", "green", "red"});

        var players = tables.Single(x => x.Identifier.Name == "players");
        partitioning = players.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.HasExistingDefault.ShouldBeFalse();
        partitioning.Partitions.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(new []{"blue", "green", "red"});

    }



    [Fact]
    public async Task apply_additive_migration()
    {
        var database = new ManagedListDatabase();
        var partitions = new Dictionary<string, string> { { "red", "red" }, { "green", "green" }, { "blue", "blue" }, };
        await database.Partitions.ResetValues(database, partitions, CancellationToken.None);

        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        await database.Partitions.AddPartitionToAllTables(database, "purple", "purple", CancellationToken.None);

        var tables = await database.FetchExistingTablesAsync();

        var teams = tables.Single(x => x.Identifier.Name == "teams");
        var partitioning = teams.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.HasExistingDefault.ShouldBeFalse();
        partitioning.Partitions.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(new []{"blue", "green", "purple", "red"});

        var players = tables.Single(x => x.Identifier.Name == "players");
        partitioning = players.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.HasExistingDefault.ShouldBeFalse();
        partitioning.Partitions.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(new []{"blue", "green", "purple", "red"});
    }

    [Fact]
    public async Task remove_partitions_at_runtime_smoke_test()
    {
        var database = new ManagedListDatabase();
        var partitions = new Dictionary<string, string> { { "red", "red" }, { "green", "green" }, { "blue", "blue" }, };
        await database.Partitions.ResetValues(database, partitions, CancellationToken.None);

        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        await database.Partitions.AddPartitionToAllTables(database, "purple", "purple", CancellationToken.None);

        await database.Partitions.DropPartitionFromAllTables(database, NullLogger.Instance, ["red"], CancellationToken.None);

        var tables = await database.FetchExistingTablesAsync();

        var teams = tables.Single(x => x.Identifier.Name == "teams");
        var partitioning = teams.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.HasExistingDefault.ShouldBeFalse();
        partitioning.Partitions.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(new []{"blue", "green", "purple"});

        var players = tables.Single(x => x.Identifier.Name == "players");
        partitioning = players.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.HasExistingDefault.ShouldBeFalse();
        partitioning.Partitions.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(new []{"blue", "green", "purple"});
    }

    [Fact]
    public async Task remove_partitions_by_value_at_runtime_smoke_test()
    {
        var database = new ManagedListDatabase();
        var partitions = new Dictionary<string, string> { { "red", "red_suffix" }, { "green", "green_suffix" }, { "blue", "blue_suffix" }, };
        await database.Partitions.ResetValues(database, partitions, CancellationToken.None);

        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        await database.Partitions.AddPartitionToAllTables(database, "purple", "purple_suffix", CancellationToken.None);

        await database.Partitions.DropPartitionFromAllTablesForValue(database, NullLogger.Instance, "red", CancellationToken.None);

        var tables = await database.FetchExistingTablesAsync();

        var teams = tables.Single(x => x.Identifier.Name == "teams");
        var partitioning = teams.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.HasExistingDefault.ShouldBeFalse();
        partitioning.Partitions.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(new []{"blue_suffix", "green_suffix", "purple_suffix"});

        var players = tables.Single(x => x.Identifier.Name == "players");
        partitioning = players.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.HasExistingDefault.ShouldBeFalse();
        partitioning.Partitions.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(new []{"blue_suffix", "green_suffix", "purple_suffix"});
    }

    [Fact]
    public async Task apply_additive_migration_2()
    {
        var database = new ManagedListDatabase();
        var partitions = new Dictionary<string, string> { { "red", "red" }, { "green", "green" }, { "blue", "blue" }, };
        await database.Partitions.ResetValues(database, partitions, CancellationToken.None);

        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // RECREATE a new instance of the database
        database = new ManagedListDatabase();

        await database.Partitions.AddPartitionToAllTables(database, "purple", "purple", CancellationToken.None);

        var tables = await database.FetchExistingTablesAsync();

        var teams = tables.Single(x => x.Identifier.Name == "teams");
        var partitioning = teams.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.HasExistingDefault.ShouldBeFalse();
        partitioning.Partitions.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(new []{"blue", "green", "purple", "red"});

        var players = tables.Single(x => x.Identifier.Name == "players");
        partitioning = players.Partitioning.ShouldBeOfType<ListPartitioning>();
        partitioning.HasExistingDefault.ShouldBeFalse();
        partitioning.Partitions.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(new []{"blue", "green", "purple", "red"});
    }

    [Fact]
    public async Task ignore_partitions_in_migration_no_destructive_rebuild_over_existing_partitions()
    {
        // JasperFx/marten#4706: a managed-partition table marked IgnorePartitionsInMigration must NOT be
        // destructively rebuilt by the generic schema diff when it already has its managed partitions.
        await dropManagedListsSchema();
        var database = new ManagedListDatabase(ignorePartitionsInMigration: true);
        var partitions = new Dictionary<string, string> { { "red", "red" }, { "green", "green" }, { "blue", "blue" }, };
        await database.Partitions.ResetValues(database, partitions, CancellationToken.None);

        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Re-applying over the existing partitions is a no-op — no pending change, no rebuild.
        var migration = await database.CreateMigrationAsync();
        migration.Difference.ShouldBe(SchemaPatchDifference.None);

        // And a second apply must not throw (pre-fix this rebuilt the table — CREATE _temp / copy).
        await database.AssertDatabaseMatchesConfigurationAsync();
        await database.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    [Fact]
    public async Task additive_partition_add_still_works_when_partitions_are_ignored_in_migration()
    {
        // The companion to the rebuild guard: the explicit AddPartitionToAllTables path must still
        // create partitions even though the tables set IgnorePartitionsInMigration for the generic diff
        // (the additive path clears the flag locally to compute the missing partitions to add).
        await dropManagedListsSchema();
        var database = new ManagedListDatabase(ignorePartitionsInMigration: true);
        var partitions = new Dictionary<string, string> { { "red", "red" }, { "green", "green" }, };
        await database.Partitions.ResetValues(database, partitions, CancellationToken.None);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Unique suffix so the assertion is independent of any partitions left by sibling tests.
        var suffix = "added_" + Guid.NewGuid().ToString("N")[..8];
        await database.Partitions.AddPartitionToAllTables(database, suffix, suffix, CancellationToken.None);

        // The child partition for the new value must physically exist on both managed tables.
        (await partitionExists("teams", suffix)).ShouldBeTrue($"teams_{suffix} partition should have been created");
        (await partitionExists("players", suffix)).ShouldBeTrue($"players_{suffix} partition should have been created");
    }

    private static async Task dropManagedListsSchema()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await conn.DropSchemaAsync("managed_lists");
    }

    private static async Task<bool> partitionExists(string parent, string suffix)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        var count = (long)(await conn.CreateCommand(
                "select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'managed_lists' and c.relname = :name")
            .With("name", $"{parent}_{suffix}")
            .ExecuteScalarAsync())!;
        return count > 0;
    }
}

public class ManagedListDatabase: PostgresqlDatabase
{
    public readonly ManagedListPartitions Partitions = new("Partitions",
        new DbObjectName("managed_lists", "partitions"));

    private readonly People _feature;

    public ManagedListDatabase(bool ignorePartitionsInMigration = false) : base(new DefaultMigrationLogger(), JasperFx.AutoCreate.CreateOrUpdate, new PostgresqlMigrator(), "Partitions", NpgsqlDataSource.Create(ConnectionSource.ConnectionString))
    {
        _feature = new People(Partitions, ignorePartitionsInMigration);
    }

    public override IFeatureSchema[] BuildFeatureSchemas()
    {
        return [_feature];
    }


}

public class People: FeatureSchemaBase
{
    private readonly Table _teams;
    private readonly Table _players;

    public People(ManagedListPartitions partitions, bool ignorePartitionsInMigration = false) : base("People", new PostgresqlMigrator())
    {
        _teams = new Table(new DbObjectName("managed_lists", "teams"));
        _teams.AddColumn<string>("name").AsPrimaryKey();
        _teams.AddColumn<string>("color").AsPrimaryKey().NotNull();

        _teams
            .PartitionByList("color")
            .UsePartitionManager(partitions);

        _players = new Table(new DbObjectName("managed_lists", "players"));
        _players.AddColumn<string>("name").AsPrimaryKey();
        _players.AddColumn<string>("color").AsPrimaryKey().NotNull();

        _players
            .PartitionByList("color")
            .UsePartitionManager(partitions);

        // #4706: tables whose managed partitions are reconciled out-of-band exempt the generic schema
        // diff from rebuilding them over their externally-added partitions.
        _teams.IgnorePartitionsInMigration = ignorePartitionsInMigration;
        _players.IgnorePartitionsInMigration = ignorePartitionsInMigration;
    }

    protected override IEnumerable<ISchemaObject> schemaObjects()
    {
        yield return _teams;
        yield return _players;
    }
}
