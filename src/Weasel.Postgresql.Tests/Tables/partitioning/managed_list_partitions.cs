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
}

public class ManagedListDatabase: PostgresqlDatabase
{
    public readonly ManagedListPartitions Partitions = new("Partitions",
        new DbObjectName("managed_lists", "partitions"));

    private readonly People _feature;

    public ManagedListDatabase() : base(new DefaultMigrationLogger(), AutoCreate.CreateOrUpdate, new PostgresqlMigrator(), "Partitions", NpgsqlDataSource.Create(ConnectionSource.ConnectionString))
    {
        _feature = new People(Partitions);
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

    public People(ManagedListPartitions partitions) : base("People", new PostgresqlMigrator())
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

    }

    protected override IEnumerable<ISchemaObject> schemaObjects()
    {
        yield return _teams;
        yield return _players;
    }
}
