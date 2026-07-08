using System.Linq;
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
    public async Task remove_partition_with_hyphenated_suffix_is_symmetric_with_create()
    {
        // JasperFx/weasel#338: the CREATE path sanitizes the suffix (ListPartition.SanitizeSuffix
        // lowercases and replaces every char outside [a-z0-9_] with '_') so a tenant id containing '-'
        // (hyphenated ids, and GUID-shaped ids — the common real-world case) yields a valid UNQUOTED
        // partition table name "<parent>_<sanitized>". The DROP path did NOT apply the same
        // normalization: it built "<parent>_<raw-suffix>" straight from the raw id, so it either emitted
        // invalid DDL (unquoted '-' => 42601 syntax error) or targeted a table name that create never
        // produced. Drop must resolve the identical table name as create.
        var database = new ManagedListDatabase();

        // A GUID-shaped tenant id — contains '-', the common real-world case.
        var tenantId = Guid.NewGuid().ToString();
        var sanitized = tenantId.Replace('-', '_').ToLowerInvariant();

        // ResetValues migrates the managed-partition metadata table and records the raw suffix.
        await database.Partitions.ResetValues(database,
            new Dictionary<string, string> { { tenantId, tenantId } }, CancellationToken.None);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Create sanitized the suffix, so the physical partition tables are "<parent>_<sanitized>".
        (await partitionExists("teams", sanitized)).ShouldBeTrue($"teams_{sanitized} should have been created");
        (await partitionExists("players", sanitized)).ShouldBeTrue($"players_{sanitized} should have been created");

        // Pre-fix this threw 42601 (unquoted '-') or silently missed the real partition table.
        await database.Partitions.DropPartitionFromAllTablesForValue(database, NullLogger.Instance, tenantId, CancellationToken.None);

        (await partitionExists("teams", sanitized)).ShouldBeFalse($"teams_{sanitized} should have been dropped");
        (await partitionExists("players", sanitized)).ShouldBeFalse($"players_{sanitized} should have been dropped");
    }

    [Fact]
    public async Task drop_partition_when_parent_table_is_not_physically_present()
    {
        // JasperFx/weasel#344: a configured partition-parent table may never have been physically migrated
        // onto this database (e.g. a registered tenant whose doc/event tables don't exist on this shard yet).
        // Dropping the partition must NOT throw 42P01 for the missing parent — there is genuinely nothing to
        // drop, and the registry row still has to be removed.
        await dropManagedListsSchema();

        var database = new ManagedListDatabase();

        // Register the partition value WITHOUT applying schema, so the teams/players parent tables are never
        // physically created — only the managed-partition metadata table exists.
        await database.Partitions.ResetValues(database,
            new Dictionary<string, string> { { "red", "red" } }, CancellationToken.None);

        (await tableExists("teams")).ShouldBeFalse("parent table must not be physically present for this repro");
        (await tableExists("players")).ShouldBeFalse("parent table must not be physically present for this repro");

        // Pre-fix this threw 42P01 (relation "managed_lists.teams" does not exist) from the fallback DETACH.
        await Should.NotThrowAsync(() =>
            database.Partitions.DropPartitionFromAllTables(database, NullLogger.Instance, ["red"], CancellationToken.None));

        // The registry row must still have been removed. Force a reload so we read from the database rather
        // than the in-memory cache.
        database.Partitions.ForceReload();
        await database.Partitions.InitializeAsync(database, CancellationToken.None);
        database.Partitions.Partitions.ShouldNotContainKey("red");
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

    [Fact]
    public async Task additive_partition_provisioning_restores_ignore_partitions_flag()
    {
        // JasperFx/marten#4713: AddPartitionToAllTables temporarily clears IgnorePartitionsInMigration
        // to reconcile the managed partitions, but it MUST restore it afterward. These Table objects are
        // reused across schema applies — in a Marten consumer they are per-mapping singletons shared
        // across every shard database. Leaving the flag cleared defeats the #4706 short-circuit on the
        // shared instance, so a later ApplyAllConfiguredChangesToDatabaseAsync re-emits
        // CREATE TABLE ... partition of for already-existing per-tenant partitions -> 42P07.
        await dropManagedListsSchema();
        var database = new ManagedListDatabase(ignorePartitionsInMigration: true);
        var partitions = new Dictionary<string, string> { { "red", "red" } };
        await database.Partitions.ResetValues(database, partitions, CancellationToken.None);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Provision a new tenant partition out-of-band — this is the call that clears the flag.
        await database.Partitions.AddPartitionToAllTables(database, "green", "green", CancellationToken.None);

        // The managed tables (the same instances the additive path operated on) must keep
        // IgnorePartitionsInMigration set after the reconcile.
        var managedTables = database.AllObjects().OfType<Table>()
            .Where(t => t.Partitioning is ListPartitioning { PartitionManager: not null }).ToArray();
        managedTables.ShouldNotBeEmpty();
        foreach (var table in managedTables)
        {
            table.IgnorePartitionsInMigration.ShouldBeTrue(
                $"{table.Identifier} must keep IgnorePartitionsInMigration after additive provisioning (#4713)");
        }

        // And a re-apply over the provisioned partitions stays a no-op (no 42P07 re-create).
        var migration = await database.CreateMigrationAsync();
        migration.Difference.ShouldBe(SchemaPatchDifference.None);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    [Fact]
    public async Task adds_requested_partition_even_when_table_has_partitions_outside_the_managers_set()
    {
        // Regression for managed partitions living in sharded databases: the manager's in-memory partition
        // set is the UNION of tenant values seen across ALL databases, so an individual database's table can
        // hold partitions the manager's set does not include. Pre-fix, ListPartitioning.CreateDelta treated
        // those extra partitions as PartitionDelta.Rebuild and additivelyMigrateTablesForNewPartitions
        // skipped adding the genuinely-missing partition, so a new tenant's first write failed with 23514
        // "no partition of relation ... found for row". The out-of-band add must create exactly the
        // requested partition regardless of what else the table already holds.
        await dropManagedListsSchema();
        var database = new ManagedListDatabase();
        await database.Partitions.ResetValues(database,
            new Dictionary<string, string> { { "red", "red" } }, CancellationToken.None);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // A partition that exists on THIS database's tables but is unknown to the manager, as if it were
        // created for a tenant that lives on another shard database.
        await using (var conn = new NpgsqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync();
            await conn.CreateCommand(
                "create table managed_lists.teams_orphan partition of managed_lists.teams for values in ('orphan')")
                .ExecuteNonQueryAsync();
            await conn.CreateCommand(
                "create table managed_lists.players_orphan partition of managed_lists.players for values in ('orphan')")
                .ExecuteNonQueryAsync();
        }

        // Adding a brand-new value must still create ITS partition — not be skipped because of the orphan.
        var fresh = new ManagedListDatabase();
        await fresh.Partitions.AddPartitionToAllTables(fresh, "blue", "blue", CancellationToken.None);

        (await partitionExists("teams", "blue")).ShouldBeTrue("the requested partition must be created");
        (await partitionExists("players", "blue")).ShouldBeTrue("the requested partition must be created");
        // Purely additive: the out-of-band add must not drop pre-existing or unmanaged partitions.
        (await partitionExists("teams", "red")).ShouldBeTrue();
        (await partitionExists("teams", "orphan")).ShouldBeTrue();
    }

    private static async Task dropManagedListsSchema()
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await conn.DropSchemaAsync("managed_lists");
    }

    private static async Task<bool> tableExists(string name)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        var count = (long)(await conn.CreateCommand(
                "select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'managed_lists' and c.relname = :name")
            .With("name", name)
            .ExecuteScalarAsync())!;
        return count > 0;
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
