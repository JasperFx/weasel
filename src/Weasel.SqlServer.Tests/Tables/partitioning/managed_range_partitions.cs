using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Partitioning;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables.partitioning;

/// <summary>
///     Rolling time-window RANGE partitioning on SQL Server: NEXT USED + SPLIT RANGE to roll the leading
///     edge forward, partition TRUNCATE + MERGE RANGE to retire the trailing edge. Weasel #401.
/// </summary>
[Collection("integration")]
public class managed_range_partitions: IntegrationContext
{
    // Mid-July so neither edge of the window sits on a period boundary.
    private static readonly DateTimeOffset July = new(2026, 7, 15, 12, 0, 0, TimeSpan.Zero);

    public managed_range_partitions(): base("mrp")
    {
    }

    // ---------------------------------------------------------------------
    // Unit — no database
    // ---------------------------------------------------------------------

    [Fact]
    public void boundaries_are_the_period_starts_plus_a_trailing_bound()
    {
        var manager = theManager(July, RollingWindowPolicy.Monthly(1, 2));

        // May, June, July, August starts... plus September 1st, which keeps the top partition beyond
        // everything provisioned and therefore empty, so rolling forward is a metadata-only SPLIT.
        manager.Boundaries().ShouldBe([
            "'2026-05-01 00:00:00'",
            "'2026-06-01 00:00:00'",
            "'2026-07-01 00:00:00'",
            "'2026-08-01 00:00:00'",
            "'2026-09-01 00:00:00'"
        ]);
    }

    [Fact]
    public void create_ddl_carries_the_window()
    {
        var table = theMetricsTable(theManager(July, RollingWindowPolicy.Monthly(0, 1)));

        var writer = new StringWriter();
        table.WriteCreateStatement(new SqlServerMigrator(), writer);
        var ddl = writer.ToString();

        ddl.ShouldContain(
            "CREATE PARTITION FUNCTION [pf_metrics_occurred_at] (datetime2) AS RANGE RIGHT FOR VALUES ('2026-06-01 00:00:00', '2026-07-01 00:00:00', '2026-08-01 00:00:00');");
        ddl.ShouldContain(
            "CREATE PARTITION SCHEME [ps_metrics_occurred_at] AS PARTITION [pf_metrics_occurred_at] ALL TO ([PRIMARY]);");
        ddl.ShouldContain("ON [ps_metrics_occurred_at]([occurred_at])");
    }

    [Fact]
    public void rolling_forward_is_additive_never_rebuild()
    {
        var manager = theManager(July.AddMonths(1), RollingWindowPolicy.Monthly(1, 2));

        // What the database holds is the window as it stood in July.
        var actual = theActualInfo(theManager(July, RollingWindowPolicy.Monthly(1, 2)));

        manager.CreateDelta(actual).ShouldBe(PartitionDelta.Additive);
    }

    [Fact]
    public void boundaries_that_have_aged_out_of_the_window_are_not_drift()
    {
        var manager = theManager(July, RollingWindowPolicy.Monthly(1, 2));

        // Everything the window wants, plus a pile of boundaries from periods that have since aged out.
        var actual = theActualInfo(manager);
        actual.BoundaryValues.Insert(0, "'2025-01-01 00:00:00'");
        actual.BoundaryValues.Insert(0, "'2024-01-01 00:00:00'");

        // Retention retires those, not a rebuild of the partition function and every table on it.
        manager.CreateDelta(actual).ShouldBe(PartitionDelta.None);
    }

    [Fact]
    public void a_changed_partition_column_is_still_a_rebuild()
    {
        var manager = theManager(July, RollingWindowPolicy.Monthly(1, 2));

        var actual = theActualInfo(manager);
        actual.Column = "something_else";

        manager.CreateDelta(actual).ShouldBe(PartitionDelta.Rebuild);
    }

    [Fact]
    public void split_statements_only_cover_the_boundaries_that_are_missing()
    {
        var manager = theManager(July.AddMonths(1), RollingWindowPolicy.Monthly(1, 2));
        var actual = theActualInfo(theManager(July, RollingWindowPolicy.Monthly(1, 2)));

        var writer = new StringWriter();
        manager.WriteSplitStatements(writer, theMetricsTable(manager), actual);
        var sql = writer.ToString();

        sql.ShouldContain("ALTER PARTITION SCHEME [ps_metrics_occurred_at] NEXT USED [PRIMARY];");
        sql.ShouldContain(
            "ALTER PARTITION FUNCTION [pf_metrics_occurred_at]() SPLIT RANGE ('2026-10-01 00:00:00');");
        sql.ShouldNotContain("'2026-09-01 00:00:00'");
    }

    // ---------------------------------------------------------------------
    // Integration
    // ---------------------------------------------------------------------

    [Fact]
    public async Task the_whole_window_is_provisioned_when_the_table_is_created()
    {
        var manager = theManager(July, RollingWindowPolicy.Monthly(1, 2));
        await resetSchemaAndPartitionObjects();
        await CreateSchemaObjectInDatabase(theMetricsTable(manager));

        (await actualBoundaries()).ShouldBe([
            new DateTime(2026, 5, 1), new DateTime(2026, 6, 1), new DateTime(2026, 7, 1),
            new DateTime(2026, 8, 1), new DateTime(2026, 9, 1)
        ]);
    }

    [Fact]
    public async Task migrating_twice_without_moving_the_clock_is_a_no_op()
    {
        var manager = theManager(July, RollingWindowPolicy.Monthly(1, 2));
        await resetSchemaAndPartitionObjects();

        var database = theDatabase(manager);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        var migration = await database.CreateMigrationAsync();

        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task rolling_forward_through_migration_splits_and_preserves_data()
    {
        var clock = new TestClock(July);
        var manager = new ManagedRangePartitions(RollingWindowPolicy.Monthly(1, 2), "occurred_at", "datetime2", clock);

        await resetSchemaAndPartitionObjects();

        var database = theDatabase(manager);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        await insertMetric(1, new DateTime(2026, 7, 20));

        clock.UtcNow = July.AddMonths(1);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // The August window added exactly one boundary at the leading edge, and nothing that already
        // existed was disturbed — aged periods are retired by retention, not by migration.
        (await actualBoundaries()).ShouldBe([
            new DateTime(2026, 5, 1), new DateTime(2026, 6, 1), new DateTime(2026, 7, 1),
            new DateTime(2026, 8, 1), new DateTime(2026, 9, 1), new DateTime(2026, 10, 1)
        ]);

        (await countMetrics()).ShouldBe(1);
    }

    [Fact]
    public async Task aged_partitions_are_truncated_and_merged_away()
    {
        var clock = new TestClock(July);
        var manager = new ManagedRangePartitions(RollingWindowPolicy.Monthly(1, 2), "occurred_at", "datetime2", clock);

        await resetSchemaAndPartitionObjects();

        var database = theDatabase(manager);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        await insertMetric(1, new DateTime(2026, 5, 10));
        await insertMetric(2, new DateTime(2026, 6, 10));
        await insertMetric(3, new DateTime(2026, 7, 10));

        // Two months on the retention floor is 2026-07-01, so May and June have aged out.
        clock.UtcNow = July.AddMonths(2);

        await manager.DropAgedPartitionsAsync(database, NullLogger.Instance, CancellationToken.None);

        (await actualBoundaries()).ShouldBe([
            new DateTime(2026, 7, 1), new DateTime(2026, 8, 1), new DateTime(2026, 9, 1)
        ]);

        // TRUNCATE deallocated the aged partitions' pages — that O(1) reclaim is the whole point.
        (await countMetrics()).ShouldBe(1);
    }

    [Fact]
    public async Task rows_older_than_the_oldest_boundary_are_reclaimed_too()
    {
        var clock = new TestClock(July);
        var manager = new ManagedRangePartitions(RollingWindowPolicy.Monthly(1, 2), "occurred_at", "datetime2", clock);

        await resetSchemaAndPartitionObjects();

        var database = theDatabase(manager);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // A late-arriving row dated before the window ever started. It lands in the underflow partition,
        // which is entirely below the retention floor once the oldest boundary ages out.
        await insertMetric(1, new DateTime(2024, 3, 3));
        await insertMetric(2, new DateTime(2026, 7, 10));

        clock.UtcNow = July.AddMonths(2);
        await manager.DropAgedPartitionsAsync(database, NullLogger.Instance, CancellationToken.None);

        (await countMetrics()).ShouldBe(1);
    }

    [Fact]
    public async Task retention_leaves_boundaries_this_policy_does_not_own_alone()
    {
        var clock = new TestClock(July);
        var manager = new ManagedRangePartitions(RollingWindowPolicy.Monthly(1, 2), "occurred_at", "datetime2", clock);

        await resetSchemaAndPartitionObjects();

        var database = theDatabase(manager);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        // A hand-added mid-month boundary well below the retention floor. It is not a period start for
        // this policy, so Weasel did not create it and Weasel does not get to merge it away.
        await theConnection.CreateCommand(
                "ALTER PARTITION SCHEME [ps_metrics_occurred_at] NEXT USED [PRIMARY];ALTER PARTITION FUNCTION [pf_metrics_occurred_at]() SPLIT RANGE ('2026-05-15 00:00:00');")
            .ExecuteNonQueryAsync();

        clock.UtcNow = July.AddMonths(2);
        await manager.DropAgedPartitionsAsync(database, NullLogger.Instance, CancellationToken.None);

        (await actualBoundaries()).ShouldContain(new DateTime(2026, 5, 15));
    }

    [Fact]
    public async Task apply_rolls_the_leading_edge_forward_and_retires_the_trailing_edge_and_is_idempotent()
    {
        var clock = new TestClock(July);
        var manager = new ManagedRangePartitions(RollingWindowPolicy.Monthly(1, 2), "occurred_at", "datetime2", clock);

        await resetSchemaAndPartitionObjects();

        var database = theDatabase(manager);
        await database.ApplyAllConfiguredChangesToDatabaseAsync();

        clock.UtcNow = July.AddMonths(2);

        var statuses = await manager.ApplyAsync(database, NullLogger.Instance, CancellationToken.None);
        statuses.Single().Status.ShouldBe(PartitionMigrationStatus.Complete);

        // A second pass changes nothing and throws nothing — safe on every startup.
        await manager.ApplyAsync(database, NullLogger.Instance, CancellationToken.None);

        (await actualBoundaries()).ShouldBe([
            new DateTime(2026, 7, 1), new DateTime(2026, 8, 1), new DateTime(2026, 9, 1),
            new DateTime(2026, 10, 1), new DateTime(2026, 11, 1)
        ]);
    }

    [Fact]
    public async Task skips_a_table_whose_partition_function_is_not_present()
    {
        var manager = theManager(July, RollingWindowPolicy.Monthly(1, 2));
        await resetSchemaAndPartitionObjects();

        // Configured, but never migrated onto this database.
        var statuses = await manager.ApplyAsync(theDatabase(manager), NullLogger.Instance, CancellationToken.None);

        statuses.ShouldBeEmpty();
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static ManagedRangePartitions theManager(DateTimeOffset now, RollingWindowPolicy policy)
        => new(policy, "occurred_at", "datetime2", new TestClock(now));

    private static Table theMetricsTable(ManagedRangePartitions manager)
    {
        var table = new Table("mrp.metrics");
        table.AddColumn<int>("id");
        table.AddColumn("occurred_at", "datetime2").NotNull();
        table.AddColumn("value", "float");

        // The partition column has to participate in the primary key on SQL Server.
        table.ModifyColumn("id").AsPrimaryKey();
        table.ModifyColumn("occurred_at").AsPrimaryKey();

        table.PartitionByRollingWindow(manager);

        return table;
    }

    private static DatabaseWithTables theDatabase(ManagedRangePartitions manager)
    {
        var database = new DatabaseWithTables("mrp_integration", ConnectionSource.ConnectionString);
        database.AddTable(theMetricsTable(manager));

        return database;
    }

    /// <summary>
    /// The partition info a database holding <paramref name="manager"/>'s window would report.
    /// </summary>
    private static SqlServerPartitionInfo theActualInfo(ManagedRangePartitions manager)
        => new()
        {
            PartitionFunctionName = "pf_metrics_occurred_at",
            PartitionSchemeName = "ps_metrics_occurred_at",
            Column = "occurred_at",
            SqlDataType = "datetime2",
            IsRangeRight = true,
            BoundaryValues = manager.Boundaries().ToList()
        };

    private async Task resetSchemaAndPartitionObjects()
    {
        await ResetSchema();

        await theConnection.CreateCommand("""
                                          IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_metrics_occurred_at')
                                              DROP PARTITION SCHEME [ps_metrics_occurred_at];
                                          IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_metrics_occurred_at')
                                              DROP PARTITION FUNCTION [pf_metrics_occurred_at];
                                          """)
            .ExecuteNonQueryAsync();
    }

    private async Task<DateTime[]> actualBoundaries()
    {
        var values = new List<DateTime>();

        await using var reader = await theConnection.CreateCommand("""
                                                                   SELECT prv.value
                                                                   FROM sys.partition_functions pf
                                                                   JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id
                                                                   WHERE pf.name = 'pf_metrics_occurred_at'
                                                                   ORDER BY prv.boundary_id;
                                                                   """).ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            values.Add((DateTime)reader.GetValue(0));
        }

        return values.ToArray();
    }

    private Task insertMetric(int id, DateTime occurredAt)
    {
        var command = theConnection.CreateCommand(
            "insert into mrp.metrics (id, occurred_at, value) values (@id, @at, 1.0)");
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@at", occurredAt);

        return command.ExecuteNonQueryAsync();
    }

    private async Task<int> countMetrics()
    {
        var count = await theConnection.CreateCommand("select count(*) from mrp.metrics").ExecuteScalarAsync();

        return (int)count!;
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
