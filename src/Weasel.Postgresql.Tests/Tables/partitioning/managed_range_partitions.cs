using System.Data;
using JasperFx;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Core.Partitioning;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

[Collection("managed_ranges")]
public class managed_range_partitions: IntegrationContext
{
    // Mid-July so that neither the leading nor the trailing edge of the window sits on a boundary.
    private static readonly DateTimeOffset July = new(2026, 7, 15, 12, 0, 0, TimeSpan.Zero);

    public managed_range_partitions(): base("managed_ranges")
    {
    }

    public override ValueTask InitializeAsync() => new(ResetSchema());

    [Fact]
    public void the_window_is_a_pure_function_of_policy_and_clock()
    {
        var clock = new TestClock(July);
        var manager = new ManagedRangePartitions(RollingWindowPolicy.Monthly(1, 2), clock);

        manager.Partitions().Select(x => x.Suffix)
            .ShouldBe(["m202605", "m202606", "m202607", "m202608"]);

        clock.UtcNow = July.AddMonths(1);

        manager.Partitions().Select(x => x.Suffix)
            .ShouldBe(["m202606", "m202607", "m202608", "m202609"]);
    }

    [Fact]
    public async Task the_whole_window_is_provisioned_when_the_table_is_created()
    {
        await using var database = new ManagedRangeDatabase(July);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var partitioning = await fetchPartitioning(database);

        partitioning.Ranges.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(["m202605", "m202606", "m202607", "m202608"]);
        partitioning.HasExistingDefault.ShouldBeTrue();
    }

    [Fact]
    public async Task migrating_twice_without_moving_the_clock_is_a_no_op()
    {
        await using var database = new ManagedRangeDatabase(July);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var migration = await database.CreateMigrationAsync();

        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task rolling_the_window_forward_is_additive_never_rebuild()
    {
        await using var database = new ManagedRangeDatabase(July);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        database.Clock.UtcNow = July.AddMonths(1);

        var delta = await fetchTableDelta(database);

        delta.PartitionDelta.ShouldBe(PartitionDelta.Additive);
        delta.MissingPartitions.OfType<RangePartition>().Select(x => x.Suffix)
            .ShouldBe(["m202609"]);
    }

    [Fact]
    public async Task partitions_that_have_aged_out_of_the_window_are_not_drift()
    {
        await using var database = new ManagedRangeDatabase(July);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Six months on, EVERY partition the database holds has fallen out of the declared window. The
        // declarative RANGE strategy reads that as "the actual table has partitions we no longer expect"
        // and rebuilds; a rolling window must not, because that is a multi-gigabyte table copy triggered
        // by nothing more than the calendar. weasel#401.
        database.Clock.UtcNow = July.AddMonths(6);

        var delta = await fetchTableDelta(database);

        delta.PartitionDelta.ShouldBe(PartitionDelta.Additive);
    }

    [Fact]
    public async Task rolling_forward_through_migration_preserves_the_existing_data()
    {
        await using var database = new ManagedRangeDatabase(July);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        await insertEvent("in-july", July);

        database.Clock.UtcNow = July.AddMonths(1);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        (await countEvents()).ShouldBe(1);

        var partitioning = await fetchPartitioning(database);

        // The new leading-edge partition was added, and nothing that already existed was disturbed —
        // aged partitions are removed by the retention pass, not by migration.
        partitioning.Ranges.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(["m202605", "m202606", "m202607", "m202608", "m202609"]);
    }

    [Fact]
    public async Task apply_provisions_the_window_out_of_band_and_is_idempotent()
    {
        await using var database = new ManagedRangeDatabase(July);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        database.Clock.UtcNow = July.AddMonths(1);

        var statuses = await database.Partitions.ApplyAsync(database, NullLogger.Instance, CancellationToken.None);
        statuses.Single().Status.ShouldBe(PartitionMigrationStatus.Complete);

        // Running it a second time changes nothing and throws nothing — this is what makes it safe on
        // every startup and from every node.
        await database.Partitions.ApplyAsync(database, NullLogger.Instance, CancellationToken.None);

        var partitioning = await fetchPartitioning(database);
        partitioning.Ranges.Select(x => x.Suffix).OrderBy(x => x)
            .ShouldBe(["m202606", "m202607", "m202608", "m202609"]);
    }

    [Fact]
    public async Task aged_partitions_are_dropped_by_the_retention_pass()
    {
        await using var database = new ManagedRangeDatabase(July);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        await insertEvent("may", new DateTimeOffset(2026, 5, 10, 0, 0, 0, TimeSpan.Zero));
        await insertEvent("july", July);

        // Two months on, the retention floor is 2026-07-01, so May and June are aged out.
        database.Clock.UtcNow = July.AddMonths(2);

        await database.Partitions.DropAgedPartitionsAsync(database, NullLogger.Instance, CancellationToken.None);

        // Retention only retires — provisioning the leading edge is RollForwardAsync's job — so the two
        // partitions that are still inside the window survive untouched.
        (await partitionTableNames())
            .ShouldBe(["events_default", "events_m202607", "events_m202608"]);

        // Dropping the partition is what reclaims the storage — the May row went with it.
        (await countEvents()).ShouldBe(1);
    }

    [Fact]
    public async Task apply_rolls_the_leading_edge_forward_and_retires_the_trailing_edge_in_one_pass()
    {
        await using var database = new ManagedRangeDatabase(July);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        database.Clock.UtcNow = July.AddMonths(2);

        await database.Partitions.ApplyAsync(database, NullLogger.Instance, CancellationToken.None);

        (await partitionTableNames())
            .ShouldBe(["events_default", "events_m202607", "events_m202608", "events_m202609", "events_m202610"]);
    }

    [Fact]
    public async Task two_nodes_provisioning_the_same_window_at_once_is_safe()
    {
        await using var database = new ManagedRangeDatabase(July);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Separate manager instances standing in for separate application nodes, racing on the same
        // window. CREATE TABLE IF NOT EXISTS is not atomic against a concurrent creator, so this is the
        // case that has to not throw.
        await using var nodeA = new ManagedRangeDatabase(July.AddMonths(3));
        await using var nodeB = new ManagedRangeDatabase(July.AddMonths(3));

        await Task.WhenAll(
            nodeA.Partitions.RollForwardAsync(nodeA, NullLogger.Instance, CancellationToken.None),
            nodeB.Partitions.RollForwardAsync(nodeB, NullLogger.Instance, CancellationToken.None));

        (await partitionTableNames()).ShouldContain("events_m202611");
    }

    [Fact]
    public async Task retention_leaves_partitions_this_policy_does_not_own_alone()
    {
        await using var database = new ManagedRangeDatabase(July);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // A hand-created archive partition well below the retention floor. Weasel did not name it, so
        // Weasel does not get to drop it.
        await ensureOpen();
        await theConnection.CreateCommand(
                "create table managed_ranges.events_archive partition of managed_ranges.events for values from ('2019-01-01 00:00:00+00') to ('2020-01-01 00:00:00+00');")
            .ExecuteNonQueryAsync();

        database.Clock.UtcNow = July.AddMonths(2);
        await database.Partitions.DropAgedPartitionsAsync(database, NullLogger.Instance, CancellationToken.None);

        (await partitionTableNames()).ShouldContain("events_archive");
    }

    [Fact]
    public async Task the_default_partition_catches_rows_outside_the_window()
    {
        await using var database = new ManagedRangeDatabase(July);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Years beyond anything provisioned. Without the DEFAULT overflow partition this insert fails
        // with 23514 "no partition of relation found for row".
        await insertEvent("far-future", new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));

        await ensureOpen();
        var count = await theConnection.CreateCommand("select count(*) from managed_ranges.events_default")
            .ExecuteScalarAsync();

        Convert.ToInt32(count).ShouldBe(1);
    }

    [Fact]
    public async Task retention_skips_a_table_that_is_not_present_on_this_database()
    {
        // A configured parent that was never physically migrated here — a sharded deployment where this
        // table does not live yet. Nothing to provision, nothing to retire, and no 42P01.
        await using var database = new ManagedRangeDatabase(July);

        var statuses = await database.Partitions.ApplyAsync(database, NullLogger.Instance, CancellationToken.None);

        statuses.ShouldBeEmpty();
    }

    [Fact]
    public void declaring_static_ranges_alongside_a_manager_is_rejected()
    {
        var table = new Table(new DbObjectName("managed_ranges", "events"));
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<DateTimeOffset>("occurred_at").AsPrimaryKey().NotNull();

        var partitioning = table.PartitionByRange("occurred_at")
            .UsePartitionManager(new ManagedRangePartitions(RollingWindowPolicy.Monthly(1, 1)));

        Should.Throw<InvalidOperationException>(() => partitioning.AddRange("twenties", 20, 29));
    }

    private async Task ensureOpen()
    {
        if (theConnection.State != ConnectionState.Open)
        {
            await theConnection.OpenAsync();
        }
    }

    private static async Task<RangePartitioning> fetchPartitioning(ManagedRangeDatabase database)
    {
        var tables = await database.FetchExistingTablesAsync();
        var table = tables.Single(x => x.Identifier.Name == "events");

        return table.Partitioning.ShouldBeOfType<RangePartitioning>();
    }

    private static async Task<TableDelta> fetchTableDelta(ManagedRangeDatabase database)
    {
        var migration = await database.CreateMigrationAsync();

        return migration.Deltas.OfType<TableDelta>().Single(x => x.Expected.Identifier.Name == "events");
    }

    private async Task<string[]> partitionTableNames()
    {
        await ensureOpen();

        var names = new List<string>();
        await using var reader = await theConnection.CreateCommand(
                """
                select c.relname
                from pg_inherits i
                join pg_class c on c.oid = i.inhrelid
                join pg_class p on p.oid = i.inhparent
                where p.relname = 'events'
                order by c.relname
                """)
            .ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            names.Add(reader.GetString(0));
        }

        return names.ToArray();
    }

    private async Task insertEvent(string name, DateTimeOffset occurredAt)
    {
        await ensureOpen();

        await theConnection
            .CreateCommand("insert into managed_ranges.events (id, occurred_at, name) values (:id, :at, :name)")
            .With("id", Guid.NewGuid())
            .With("at", occurredAt)
            .With("name", name)
            .ExecuteNonQueryAsync();
    }

    private async Task<int> countEvents()
    {
        await ensureOpen();

        var count = await theConnection.CreateCommand("select count(*) from managed_ranges.events")
            .ExecuteScalarAsync();

        return Convert.ToInt32(count);
    }
}

/// <summary>
/// Deterministic clock so the tests can roll the window forward without waiting for the calendar.
/// </summary>
public class TestClock: TimeProvider
{
    public TestClock(DateTimeOffset now) => UtcNow = now;

    public DateTimeOffset UtcNow { get; set; }

    public override DateTimeOffset GetUtcNow() => UtcNow;
}

public class ManagedRangeDatabase: PostgresqlDatabase, IAsyncDisposable
{
    private readonly Events _feature;

    public ManagedRangeDatabase(DateTimeOffset now, RollingWindowPolicy? policy = null): base(
        new DefaultMigrationLogger(), AutoCreate.CreateOrUpdate, new PostgresqlMigrator(), "Ranges",
        NpgsqlDataSource.Create(ConnectionSource.ConnectionString))
    {
        Clock = new TestClock(now);
        Partitions = new ManagedRangePartitions(policy ?? RollingWindowPolicy.Monthly(1, 2), Clock);
        _feature = new Events(Partitions);
    }

    public TestClock Clock { get; }

    public ManagedRangePartitions Partitions { get; }

    public override IFeatureSchema[] BuildFeatureSchemas() => [_feature];

    public ValueTask DisposeAsync() => DataSource.DisposeAsync();
}

public class Events: FeatureSchemaBase
{
    private readonly Table _events;

    public Events(ManagedRangePartitions partitions): base("Events", new PostgresqlMigrator())
    {
        _events = new Table(new DbObjectName("managed_ranges", "events"));
        _events.AddColumn<Guid>("id").AsPrimaryKey();

        // PostgreSQL requires the partition key to be part of every unique constraint on a partitioned
        // table, so occurred_at is in the primary key.
        _events.AddColumn<DateTimeOffset>("occurred_at").AsPrimaryKey().NotNull();
        _events.AddColumn<string>("name");

        _events.PartitionByRange("occurred_at").UsePartitionManager(partitions);
    }

    protected override IEnumerable<ISchemaObject> schemaObjects()
    {
        yield return _events;
    }
}
