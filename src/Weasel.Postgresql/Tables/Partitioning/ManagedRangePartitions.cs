using JasperFx.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Weasel.Core;
using Weasel.Core.Partitioning;

namespace Weasel.Postgresql.Tables.Partitioning;

/// <summary>
///     Supplies the partition set of a <see cref="RangePartitioning" /> at runtime instead of the table
///     declaring a static list of ranges.
/// </summary>
public interface IRangePartitionManager
{
    /// <summary>
    ///     The partitions that should exist right now.
    /// </summary>
    IEnumerable<RangePartition> Partitions();
}

/// <summary>
///     A runtime-managed RANGE partition strategy for rolling time windows — the range analogue of
///     <see cref="ManagedListPartitions" />. Consumers declare intent ("monthly, 12 ahead, retain 6")
///     through a <see cref="RollingWindowPolicy" /> and Weasel owns every DDL statement: it creates the
///     periods at the leading edge additively and drops the aged ones at the trailing edge.
///     <para>
///         This is what makes range partitioning worth having on a time-series table. Reclaiming a
///         period is a <c>DROP TABLE</c> against one partition — O(1), no mass <c>DELETE</c>, no bloat,
///         no vacuum storm. And because the window is a pure function of the policy and the clock, a
///         window that has rolled forward is never mistaken for schema drift: see
///         <see cref="RangePartitioning.CreateDelta" />, which is purely additive whenever a manager is
///         attached. No consumer needs <see cref="Table.IgnorePartitionsInMigration" /> or an
///         "externally managed" escape hatch, so nothing gives up Weasel's ordering and dependency
///         management to run a rolling time-partitioned table.
///     </para>
///     <para>
///         Usage: attach the manager to the table's range partitioning, then call
///         <see cref="ApplyAsync(PostgresqlDatabase,ILogger,CancellationToken)" /> at startup and on
///         whatever cadence the period demands. Both halves are idempotent and safe to run concurrently
///         from multiple nodes.
///     </para>
///     <code>
///     var manager = new ManagedRangePartitions(RollingWindowPolicy.Monthly(periodsAhead: 3, periodsBehind: 6));
///     table.PartitionByRange("occurred_at").UsePartitionManager(manager);
///     </code>
///     <para>JasperFx/weasel#401.</para>
/// </summary>
public class ManagedRangePartitions: IRangePartitionManager
{
    private readonly TimeProvider _timeProvider;

    /// <summary>
    ///     Create a rolling-window partition manager.
    /// </summary>
    /// <param name="policy">The rolling window: period size, periods provisioned ahead, periods retained.</param>
    /// <param name="timeProvider">
    ///     Clock used to resolve "now". Defaults to <see cref="TimeProvider.System" />; supply a fake to
    ///     roll the window forward deterministically in tests.
    /// </param>
    public ManagedRangePartitions(RollingWindowPolicy policy, TimeProvider? timeProvider = null)
    {
        Policy = policy ?? throw new ArgumentNullException(nameof(policy));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>
    ///     The rolling window this manager provisions against.
    /// </summary>
    public RollingWindowPolicy Policy { get; }

    /// <summary>
    ///     "Now" according to this manager's clock, in UTC.
    /// </summary>
    public DateTimeOffset UtcNow => _timeProvider.GetUtcNow();

    /// <summary>
    ///     The window this manager expects to exist right now, oldest period first.
    /// </summary>
    public IReadOnlyList<TimeWindowPartition> CurrentWindow() => Policy.Window(UtcNow);

    /// <inheritdoc />
    public IEnumerable<RangePartition> Partitions()
        => CurrentWindow().Select(ToRangePartition);

    /// <summary>
    ///     Translate one window period into the Weasel partition that represents it. PostgreSQL RANGE
    ///     partition bounds are already half-open — <c>FROM (x) TO (y)</c> includes x and excludes y — so
    ///     the window's interval maps across without adjustment.
    /// </summary>
    internal static RangePartition ToRangePartition(TimeWindowPartition period)
        => new(period.Suffix, period.From.FormatSqlValue(), period.To.FormatSqlValue());

    /// <summary>
    ///     Roll the window forward and retire aged partitions across every table wired to this manager.
    ///     Idempotent, and safe to run on every startup and from several nodes at once.
    /// </summary>
    public async Task<TablePartitionStatus[]> ApplyAsync(PostgresqlDatabase database, ILogger logger,
        CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        try
        {
            return await applyAsync(conn, database, logger, rollForward: true, dropAged: true, token)
                .ConfigureAwait(false);
        }
        finally
        {
            await conn.CloseAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Roll the window forward and retire aged partitions, with no logging.
    /// </summary>
    public Task<TablePartitionStatus[]> ApplyAsync(PostgresqlDatabase database, CancellationToken token)
        => ApplyAsync(database, NullLogger.Instance, token);

    /// <summary>
    ///     Create any partitions in the current window that do not exist yet, plus the DEFAULT overflow
    ///     partition. Purely additive — nothing is ever dropped here, so this is the half that is safe to
    ///     run without a retention decision.
    /// </summary>
    public async Task<TablePartitionStatus[]> RollForwardAsync(PostgresqlDatabase database, ILogger logger,
        CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        try
        {
            return await applyAsync(conn, database, logger, rollForward: true, dropAged: false, token)
                .ConfigureAwait(false);
        }
        finally
        {
            await conn.CloseAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Drop every partition older than <see cref="RollingWindowPolicy.RetentionFloor" />. Only
    ///     partitions this manager's own naming scheme produced are considered — a partition whose suffix
    ///     this policy cannot parse (hand-created, or created under a different period size) is left
    ///     strictly alone.
    ///     <para>
    ///         This is a data-removing operation by design: dropping the partition is what reclaims the
    ///         storage in O(1).
    ///     </para>
    /// </summary>
    public async Task<TablePartitionStatus[]> DropAgedPartitionsAsync(PostgresqlDatabase database, ILogger logger,
        CancellationToken token)
    {
        await using var conn = database.CreateConnection();
        await conn.OpenAsync(token).ConfigureAwait(false);

        try
        {
            return await applyAsync(conn, database, logger, rollForward: false, dropAged: true, token)
                .ConfigureAwait(false);
        }
        finally
        {
            await conn.CloseAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Every table in <paramref name="database" /> whose range partitioning is wired to this manager.
    /// </summary>
    public IReadOnlyList<Table> ResolveManagedTables(PostgresqlDatabase database)
    {
        return database
            .AllObjects()
            .OfType<Table>()
            .Where(x => x.Partitioning is RangePartitioning range && ReferenceEquals(range.PartitionManager, this))
            .ToArray();
    }

    private async Task<TablePartitionStatus[]> applyAsync(NpgsqlConnection conn, PostgresqlDatabase database,
        ILogger logger, bool rollForward, bool dropAged, CancellationToken token)
    {
        // Resolve "now" ONCE for the whole pass. Reading the clock per table could straddle a period
        // boundary and leave one table provisioned a period further ahead than another.
        var now = UtcNow;
        var window = Policy.Window(now);
        var floor = Policy.RetentionFloor(now);

        var statuses = new List<TablePartitionStatus>();

        foreach (var table in ResolveManagedTables(database))
        {
            // A configured partition parent may never have been physically migrated onto this database
            // (a sharded deployment where this table does not live yet). There is genuinely nothing to
            // provision or retire, and every statement below would fail with 42P01. Same reasoning as
            // ManagedListPartitions.DropPartitionFromAllTables (weasel#344).
            if (!await parentExistsAsync(conn, table, token).ConfigureAwait(false))
            {
                logger.LogInformation(
                    "Skipped managed range partitions for table {Table} because it is not physically present on this database",
                    table.Identifier);
                continue;
            }

            try
            {
                if (rollForward)
                {
                    await rollForwardAsync(conn, table, window, logger, token).ConfigureAwait(false);
                }

                if (dropAged)
                {
                    await dropAgedAsync(conn, table, floor, logger, token).ConfigureAwait(false);
                }

                statuses.Add(new TablePartitionStatus(table.Identifier, PartitionMigrationStatus.Complete));
            }
            catch (Exception e)
            {
                logger.LogError(e, "Error managing rolling range partitions for {Table}", table.Identifier);
                statuses.Add(new TablePartitionStatus(table.Identifier, PartitionMigrationStatus.Failed));
            }
        }

        return statuses.ToArray();
    }

    private static async Task<bool> parentExistsAsync(NpgsqlConnection conn, Table table, CancellationToken token)
    {
        var exists = await conn
            .CreateCommand("select to_regclass(:qualified) is not null")
            .With("qualified", table.Identifier.QualifiedName)
            .ExecuteScalarAsync(token).ConfigureAwait(false);

        return exists is true;
    }

    private async Task rollForwardAsync(NpgsqlConnection conn, Table table,
        IReadOnlyList<TimeWindowPartition> window, ILogger logger, CancellationToken token)
    {
        // The DEFAULT partition goes first so a row outside the provisioned window is never rejected,
        // even in the sliver of time before the window partitions land.
        var defaultWriter = new StringWriter();
        defaultWriter.WriteDefaultPartition(table.Identifier);
        await executeIdempotentCreateAsync(conn, defaultWriter.ToString(), token).ConfigureAwait(false);

        var existing = await fetchPartitionNamesAsync(conn, table, token).ConfigureAwait(false);

        foreach (var period in window)
        {
            var partitionName = table.Identifier.Name + "_" + period.Suffix;
            if (existing.Contains(partitionName))
            {
                continue;
            }

            var writer = new StringWriter();
            ((IPartition)ToRangePartition(period)).WriteCreateStatement(writer, table);

            await executeIdempotentCreateAsync(conn, writer.ToString(), token).ConfigureAwait(false);

            logger.LogInformation("Created range partition {Partition} covering [{From}, {To}) on table {Table}",
                partitionName, period.From, period.To, table.Identifier);
        }
    }

    private async Task dropAgedAsync(NpgsqlConnection conn, Table table, DateTimeOffset floor, ILogger logger,
        CancellationToken token)
    {
        var parentName = PostgresqlObjectName.From(table.Identifier);

        foreach (var partitionName in await fetchPartitionNamesAsync(conn, table, token).ConfigureAwait(false))
        {
            var suffix = table.Identifier.GetSuffixName(partitionName);

            // Only ever retire partitions this policy itself named. A suffix it cannot parse belongs to
            // somebody else — a hand-created partition, a leftover from a different period size, or the
            // DEFAULT overflow partition — and dropping it would be destroying data we were never asked
            // to manage.
            if (!Policy.TryParseSuffix(suffix, out var periodStart) || periodStart >= floor)
            {
                continue;
            }

            var child = PostgresqlObjectName.From(new DbObjectName(table.Identifier.Schema, partitionName));

            try
            {
                // Deliberately NOT "DETACH ... CONCURRENTLY": PostgreSQL rejects a concurrent detach on a
                // parent that has a DEFAULT partition, and a managed range table always has one.
                await conn.CreateCommand($"alter table {parentName} detach partition {child};")
                    .ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }
            catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                // Another node got there first. The drop below is a no-op then.
            }

            await conn.CreateCommand($"drop table if exists {child} cascade;")
                .ExecuteNonQueryAsync(token).ConfigureAwait(false);

            logger.LogInformation(
                "Dropped aged range partition {Partition} (period starting {PeriodStart}, retention floor {Floor}) from table {Table}",
                partitionName, periodStart, floor, table.Identifier);
        }
    }

    private static async Task<HashSet<string>> fetchPartitionNamesAsync(NpgsqlConnection conn, Table table,
        CancellationToken token)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        await using var reader = await conn
            .CreateCommand(
                """
                select c.relname
                from pg_inherits i
                join pg_class c on c.oid = i.inhrelid
                join pg_class p on p.oid = i.inhparent
                join pg_namespace n on n.oid = p.relnamespace
                where n.nspname = :schema and p.relname = :name
                """)
            .With("schema", table.Identifier.Schema)
            .With("name", table.Identifier.Name)
            .ExecuteReaderAsync(token).ConfigureAwait(false);

        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            names.Add(await reader.GetFieldValueAsync<string>(0, token).ConfigureAwait(false));
        }

        return names;
    }

    /// <summary>
    ///     CREATE TABLE IF NOT EXISTS is not actually atomic against a concurrent creator: PostgreSQL
    ///     checks for the name, then inserts, so two nodes provisioning the same window at the same moment
    ///     can still collide on pg_class. Swallowing those two states is what makes this safe to run from
    ///     every node on every startup.
    /// </summary>
    private static async Task executeIdempotentCreateAsync(NpgsqlConnection conn, string sql, CancellationToken token)
    {
        try
        {
            await conn.CreateCommand(sql).ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }
        catch (PostgresException e) when (e.SqlState is PostgresErrorCodes.DuplicateTable
                                              or PostgresErrorCodes.UniqueViolation)
        {
        }
    }
}
