using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Weasel.Core.Migrations;
using Weasel.Core.Partitioning;

namespace Weasel.SqlServer.Tables.Partitioning;

/// <summary>
///     A runtime-managed SQL Server RANGE partition strategy for rolling time windows — the SQL Server
///     edition of PostgreSQL's <c>Weasel.Postgresql.Tables.Partitioning.ManagedRangePartitions</c>.
///     Consumers declare intent ("monthly, 3 ahead, retain 6") through a <see cref="RollingWindowPolicy" />
///     and Weasel owns every DDL statement: <c>NEXT USED</c> + <c>SPLIT RANGE</c> to roll the leading edge
///     forward, and partition <c>TRUNCATE</c> + <c>MERGE RANGE</c> to retire the trailing edge.
///     <para>
///         Sliding-window partition management on SQL Server is otherwise a hand-rolled T-SQL chore with
///         no standard tooling — published recipes amount to "write a script that sets NEXT USED and
///         SPLITs, and another that MERGEs". This is that, owned by the application's schema model and
///         versioned with its code.
///     </para>
///     <para>
///         The window is a pure function of the policy and the clock, so a window that has rolled forward
///         is never mistaken for schema drift: <see cref="CreateDelta" /> is purely additive, and the
///         boundaries that have fallen behind the retention floor are retired by
///         <see cref="DropAgedPartitionsAsync" /> as a policy outcome rather than a rebuild.
///     </para>
///     <code>
///     var manager = new ManagedRangePartitions(
///         RollingWindowPolicy.Monthly(periodsAhead: 3, periodsBehind: 6), "occurred_at");
///     table.PartitionByRollingWindow(manager);
///     </code>
///     <para>JasperFx/weasel#401.</para>
/// </summary>
public class ManagedRangePartitions: ISplittablePartitioning
{
    private readonly TimeProvider _timeProvider;

    /// <summary>
    ///     Create a rolling-window partition strategy.
    /// </summary>
    /// <param name="policy">The rolling window: period size, periods provisioned ahead, periods retained.</param>
    /// <param name="column">
    ///     The date/time column to partition on. It must participate in the table's primary key, as SQL
    ///     Server requires of any partitioned table's unique constraints.
    /// </param>
    /// <param name="sqlDataType">
    ///     The partition function's parameter type. Defaults to <c>datetime2</c>. Use the bare type name
    ///     with no precision — that is what <c>sys.types</c> reports on read-back, and delta detection
    ///     compares against it.
    /// </param>
    /// <param name="timeProvider">
    ///     Clock used to resolve "now". Defaults to <see cref="TimeProvider.System" />; supply a fake to
    ///     roll the window forward deterministically in tests.
    /// </param>
    public ManagedRangePartitions(RollingWindowPolicy policy, string column, string sqlDataType = "datetime2",
        TimeProvider? timeProvider = null)
    {
        if (string.IsNullOrWhiteSpace(column))
        {
            throw new ArgumentException("column must not be null or blank", nameof(column));
        }

        Policy = policy ?? throw new ArgumentNullException(nameof(policy));
        Column = column;
        SqlDataType = sqlDataType;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>
    ///     The rolling window this strategy provisions against.
    /// </summary>
    public RollingWindowPolicy Policy { get; }

    /// <inheritdoc />
    public string Column { get; }

    /// <inheritdoc />
    public string SqlDataType { get; }

    /// <summary>
    ///     Filegroup every partition is mapped to, and the one handed to <c>NEXT USED</c> when the window
    ///     rolls forward. Defaults to PRIMARY.
    /// </summary>
    public string Filegroup { get; set; } = "PRIMARY";

    /// <summary>
    ///     Always RANGE RIGHT: a boundary is the inclusive lower bound of its period, which matches the
    ///     half-open <c>[From, To)</c> intervals a <see cref="RollingWindowPolicy" /> produces.
    /// </summary>
    public bool IsRangeRight => true;

    /// <summary>
    ///     "Now" according to this strategy's clock, in UTC.
    /// </summary>
    public DateTimeOffset UtcNow => _timeProvider.GetUtcNow();

    /// <summary>
    ///     The window this strategy expects to exist right now, oldest period first.
    /// </summary>
    public IReadOnlyList<TimeWindowPartition> CurrentWindow() => Policy.Window(UtcNow);

    /// <summary>
    ///     The partition function boundaries for the current window, ascending, as SQL literals.
    ///     <para>
    ///         These are the period starts PLUS the exclusive end of the newest provisioned period. That
    ///         trailing boundary is not decoration: without it the top partition would be
    ///         <c>[newest period start, +infinity)</c> and would hold the newest period's rows, so the next
    ///         roll-forward would <c>SPLIT</c> a partition containing data — a fully logged row-movement
    ///         operation. With it, the top partition is always beyond everything provisioned and therefore
    ///         empty, and every <c>SPLIT</c> stays metadata-only.
    ///     </para>
    /// </summary>
    public string[] Boundaries()
    {
        var window = CurrentWindow();

        return window.Select(x => x.From)
            .Append(window[^1].To)
            .Select(FormatBoundary)
            .ToArray();
    }

    /// <summary>
    ///     Render one boundary instant as the SQL literal this strategy declares and compares against.
    ///     Routed through the same formatter <see cref="Table.FetchExistingAsync" /> uses on read-back so
    ///     a round-trip compares equal.
    /// </summary>
    public string FormatBoundary(DateTimeOffset moment)
    {
        // Boxed deliberately, and NOT through a conditional expression: DateTime converts implicitly to
        // DateTimeOffset, so a ternary over the two would silently type itself as DateTimeOffset and
        // render every boundary in the offset-carrying shape regardless of the column's type.
        object value = moment.UtcDateTime;
        if (SqlDataType.StartsWith("datetimeoffset", StringComparison.OrdinalIgnoreCase))
        {
            value = moment.ToUniversalTime();
        }

        return RangePartitioning.FormatSqlValue(value);
    }

    /// <inheritdoc />
    public string PartitionFunctionName(Table parent) => $"pf_{parent.Identifier.Name}_{Column}";

    /// <inheritdoc />
    public string PartitionSchemeName(Table parent) => $"ps_{parent.Identifier.Name}_{Column}";

    /// <inheritdoc />
    public void WritePartitionDdl(TextWriter writer, Table parent)
    {
        var pfName = PartitionFunctionName(parent);
        var psName = PartitionSchemeName(parent);

        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{SchemaUtils.EscapeLiteral(psName)}')");
        writer.WriteLine($"    DROP PARTITION SCHEME {SchemaUtils.BracketName(psName)};");
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{SchemaUtils.EscapeLiteral(pfName)}')");
        writer.WriteLine($"    DROP PARTITION FUNCTION {SchemaUtils.BracketName(pfName)};");

        writer.Write($"CREATE PARTITION FUNCTION {SchemaUtils.BracketName(pfName)} ({SqlDataType}) AS RANGE RIGHT");
        writer.Write($" FOR VALUES ({Boundaries().Join(", ")})");
        writer.WriteLine(";");

        writer.WriteLine($"CREATE PARTITION SCHEME {SchemaUtils.BracketName(psName)} AS PARTITION {SchemaUtils.BracketName(pfName)} ALL TO ({SchemaUtils.BracketName(Filegroup)});");
    }

    /// <inheritdoc />
    public void WriteOnClause(TextWriter writer, Table parent)
    {
        writer.Write($" ON [{PartitionSchemeName(parent)}]({SchemaUtils.BracketName(Column)})");
    }

    /// <inheritdoc />
    public void WriteDropDdl(TextWriter writer, Table parent)
    {
        var psName = PartitionSchemeName(parent);
        var pfName = PartitionFunctionName(parent);

        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{SchemaUtils.EscapeLiteral(psName)}')");
        writer.WriteLine($"    DROP PARTITION SCHEME {SchemaUtils.BracketName(psName)};");
        writer.WriteLine($"IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{SchemaUtils.EscapeLiteral(pfName)}')");
        writer.WriteLine($"    DROP PARTITION FUNCTION {SchemaUtils.BracketName(pfName)};");
    }

    /// <inheritdoc />
    public PartitionDelta CreateDelta(SqlServerPartitionInfo? actual)
    {
        if (actual == null)
        {
            return PartitionDelta.Rebuild;
        }

        if (!actual.Column.EqualsIgnoreCase(Column))
        {
            return PartitionDelta.Rebuild;
        }

        if (!actual.SqlDataType.EqualsIgnoreCase(SqlDataType))
        {
            return PartitionDelta.Rebuild;
        }

        if (actual.IsRangeRight != IsRangeRight)
        {
            return PartitionDelta.Rebuild;
        }

        var expected = new HashSet<string>(Boundaries(), StringComparer.OrdinalIgnoreCase);
        var actualSet = new HashSet<string>(actual.BoundaryValues, StringComparer.OrdinalIgnoreCase);

        if (actualSet.SetEquals(expected))
        {
            return PartitionDelta.None;
        }

        // weasel#401: a boundary the declaration no longer names is a period that has aged out, which is
        // the normal steady state of a rolling window and not drift. Retiring it is the retention pass's
        // job (a MERGE RANGE against a truncated partition), never a rebuild of the partition function
        // and every table sitting on it. So migration only ever splits.
        return expected.IsSubsetOf(actualSet) ? PartitionDelta.None : PartitionDelta.Additive;
    }

    /// <inheritdoc />
    public void WriteSplitStatements(TextWriter writer, Table parent, SqlServerPartitionInfo actual)
    {
        var pfName = PartitionFunctionName(parent);
        var psName = PartitionSchemeName(parent);
        var actualSet = new HashSet<string>(actual.BoundaryValues, StringComparer.OrdinalIgnoreCase);

        foreach (var boundary in Boundaries().Where(x => !actualSet.Contains(x)))
        {
            writer.WriteLine($"ALTER PARTITION SCHEME {SchemaUtils.BracketName(psName)} NEXT USED {SchemaUtils.BracketName(Filegroup)};");
            writer.WriteLine($"ALTER PARTITION FUNCTION {SchemaUtils.BracketName(pfName)}() SPLIT RANGE ({boundary});");
        }
    }

    /// <inheritdoc />
    public void WriteMergeStatements(TextWriter writer, Table parent, SqlServerPartitionInfo actual)
    {
        var pfName = PartitionFunctionName(parent);
        var actualSet = new HashSet<string>(actual.BoundaryValues, StringComparer.OrdinalIgnoreCase);

        foreach (var boundary in Boundaries().Where(x => !actualSet.Contains(x)))
        {
            writer.WriteLine($"ALTER PARTITION FUNCTION {SchemaUtils.BracketName(pfName)}() MERGE RANGE ({boundary});");
        }
    }

    // ---------------------------------------------------------------------
    // Runtime API — mirrors the PostgreSQL ManagedRangePartitions
    // ---------------------------------------------------------------------

    /// <summary>
    ///     Roll the window forward and retire aged periods across every table wired to this strategy.
    ///     Idempotent, and safe to run on every startup.
    /// </summary>
    public async Task<TablePartitionStatus[]> ApplyAsync(IDatabase<SqlConnection> database, ILogger logger,
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
    ///     Roll the window forward and retire aged periods, with no logging.
    /// </summary>
    public Task<TablePartitionStatus[]> ApplyAsync(IDatabase<SqlConnection> database, CancellationToken token)
        => ApplyAsync(database, NullLogger.Instance, token);

    /// <summary>
    ///     Split in any boundaries of the current window that the partition function does not have yet.
    ///     Purely additive — nothing is truncated or merged here.
    /// </summary>
    public async Task<TablePartitionStatus[]> RollForwardAsync(IDatabase<SqlConnection> database, ILogger logger,
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
    ///     Retire every period below <see cref="RollingWindowPolicy.RetentionFloor" />: the partition is
    ///     truncated — an O(1) page deallocation, which is what makes retention on a partitioned table
    ///     worth having versus a mass <c>DELETE</c> — and then its boundary is merged away against an
    ///     already-empty partition, so the merge itself is metadata-only.
    ///     <para>
    ///         Only boundaries that land exactly on a period start for this policy are retired; a
    ///         hand-added boundary is left strictly alone. This is a data-removing operation by design.
    ///     </para>
    /// </summary>
    public async Task<TablePartitionStatus[]> DropAgedPartitionsAsync(IDatabase<SqlConnection> database,
        ILogger logger, CancellationToken token)
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
    ///     Every table in <paramref name="database" /> whose partitioning is wired to this strategy.
    /// </summary>
    public IReadOnlyList<Table> ResolveManagedTables(IDatabase database)
    {
        return database.AllObjects()
            .OfType<Table>()
            .Where(t => ReferenceEquals(t.SqlServerPartitioning, this))
            .ToArray();
    }

    private async Task<TablePartitionStatus[]> applyAsync(SqlConnection conn, IDatabase<SqlConnection> database,
        ILogger logger, bool rollForward, bool dropAged, CancellationToken token)
    {
        // Resolve "now" ONCE for the whole pass so two tables can never end up provisioned a period apart
        // because the clock crossed a boundary mid-loop.
        var now = UtcNow;
        var expected = Boundaries();
        var floor = Policy.RetentionFloor(now);

        var statuses = new List<TablePartitionStatus>();

        foreach (var table in ResolveManagedTables(database))
        {
            var pfName = PartitionFunctionName(table);

            try
            {
                var actual = await fetchActualBoundariesAsync(conn, pfName, token).ConfigureAwait(false);

                // A configured table that was never physically migrated onto this database has no
                // partition function to split or merge. Nothing to do, and every statement below would
                // fail — same reasoning as the PostgreSQL half skipping an absent parent table.
                if (actual == null)
                {
                    logger.LogInformation(
                        "Skipped managed range partitions for table {Table} because partition function {Function} is not present on this database",
                        table.Identifier, pfName);
                    continue;
                }

                if (rollForward)
                {
                    await rollForwardAsync(conn, table, pfName, expected, actual, logger, token)
                        .ConfigureAwait(false);
                }

                if (dropAged)
                {
                    await dropAgedAsync(conn, table, pfName, actual, floor, logger, token).ConfigureAwait(false);
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

    private async Task rollForwardAsync(SqlConnection conn, Table table, string pfName, string[] expected,
        IReadOnlyList<ActualBoundary> actual, ILogger logger, CancellationToken token)
    {
        var psName = PartitionSchemeName(table);
        var present = new HashSet<string>(actual.Select(x => x.Literal), StringComparer.OrdinalIgnoreCase);

        foreach (var boundary in expected.Where(x => !present.Contains(x)))
        {
            await using (var nextUsed = conn.CreateCommand())
            {
                nextUsed.CommandText = $"ALTER PARTITION SCHEME {SchemaUtils.BracketName(psName)} NEXT USED {SchemaUtils.BracketName(Filegroup)};";
                await nextUsed.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }

            await using (var split = conn.CreateCommand())
            {
                split.CommandText = $"ALTER PARTITION FUNCTION {SchemaUtils.BracketName(pfName)}() SPLIT RANGE ({boundary});";
                await split.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            }

            logger.LogInformation("Split partition function {Function} at boundary {Boundary} for table {Table}",
                pfName, boundary, table.Identifier);
        }
    }

    private async Task dropAgedAsync(SqlConnection conn, Table table, string pfName,
        IReadOnlyList<ActualBoundary> actual, DateTimeOffset floor, ILogger logger, CancellationToken token)
    {
        var ordered = actual.Where(x => x.Moment.HasValue).OrderBy(x => x.Moment!.Value).ToArray();

        // Only retire boundaries this policy itself would have produced. A boundary that does not land on
        // a period start belongs to somebody else, and merging it away would silently reshape a partition
        // scheme we were never asked to manage.
        var aged = ordered
            .Where(x => x.Moment!.Value < floor && Policy.StartOfPeriod(x.Moment!.Value) == x.Moment!.Value)
            .ToArray();

        if (aged.Length == 0)
        {
            return;
        }

        // Partition 1 is everything below the lowest boundary. When that lowest boundary is itself aged,
        // partition 1 lies entirely below the retention floor — rows that arrived late, dated before the
        // window ever started — and retention owns it too. Guarded, because if the lowest boundary is one
        // we do not own, partition 1 sits behind data we were told to leave alone.
        if (ReferenceEquals(ordered[0], aged[0]))
        {
            await truncatePartitionAsync(conn, table, 1, logger, token).ConfigureAwait(false);
        }

        foreach (var boundary in aged)
        {
            // Recomputed per boundary on purpose: every MERGE below renumbers the partitions.
            var partitionNumber = await resolvePartitionNumberAsync(conn, pfName, boundary.Literal, token)
                .ConfigureAwait(false);

            if (partitionNumber == null)
            {
                logger.LogWarning(
                    "Could not resolve the partition number for boundary {Boundary} of {Function} — skipping",
                    boundary.Literal, pfName);
                continue;
            }

            // Truncate BEFORE merging. MERGE RANGE on a partition that still holds rows does not reclaim
            // anything — it moves those rows into the neighbouring partition, which is the opposite of the
            // point. If the truncate fails the boundary is deliberately left in place.
            if (!await truncatePartitionAsync(conn, table, partitionNumber.Value, logger, token)
                    .ConfigureAwait(false))
            {
                continue;
            }

            await using var merge = conn.CreateCommand();
            merge.CommandText = $"ALTER PARTITION FUNCTION {SchemaUtils.BracketName(pfName)}() MERGE RANGE ({boundary.Literal});";
            await merge.ExecuteNonQueryAsync(token).ConfigureAwait(false);

            logger.LogInformation(
                "Retired aged partition at boundary {Boundary} (retention floor {Floor}) from table {Table}",
                boundary.Literal, floor, table.Identifier);
        }
    }

    private static async Task<bool> truncatePartitionAsync(SqlConnection conn, Table table, int partitionNumber,
        ILogger logger, CancellationToken token)
    {
        try
        {
            await using var truncate = conn.CreateCommand();
            truncate.CommandText =
                $"TRUNCATE TABLE {table.Identifier.QualifiedName} WITH (PARTITIONS ({partitionNumber}));";
            await truncate.ExecuteNonQueryAsync(token).ConfigureAwait(false);

            return true;
        }
        catch (SqlException e)
        {
            logger.LogError(e, "Could not truncate partition {Partition} of table {Table}", partitionNumber,
                table.Identifier);

            return false;
        }
    }

    private static async Task<int?> resolvePartitionNumberAsync(SqlConnection conn, string pfName, string literal,
        CancellationToken token)
    {
        // $PARTITION takes the partition function by name, which cannot be parameterized. pfName is
        // Weasel-generated from the table and column names, not user input.
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT $PARTITION.{SchemaUtils.BracketName(pfName)}({literal});";

        var result = await cmd.ExecuteScalarAsync(token).ConfigureAwait(false);

        return result is int number ? number : null;
    }

    /// <summary>
    ///     The boundaries a partition function actually carries, or null when no such function exists.
    ///     <c>sys.partition_range_values.value</c> is a sql_variant holding the boundary as its native CLR
    ///     type, so it is read raw and then formatted through the same canonical formatter the declared
    ///     side uses — a <c>CAST(... AS NVARCHAR)</c> would render a date in a completely different shape
    ///     and never compare equal.
    /// </summary>
    private async Task<IReadOnlyList<ActualBoundary>?> fetchActualBoundariesAsync(SqlConnection conn, string pfName,
        CancellationToken token)
    {
        var boundaries = new List<ActualBoundary>();

        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = """
                              SELECT prv.value
                              FROM sys.partition_functions pf
                              JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id
                              WHERE pf.name = @pf
                              ORDER BY prv.boundary_id;
                              """;
            cmd.Parameters.AddWithValue("@pf", pfName);

            await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                var value = reader.GetValue(0);
                boundaries.Add(new ActualBoundary(RangePartitioning.FormatSqlValue(value), AsMoment(value)));
            }
        }

        if (boundaries.Count > 0)
        {
            return boundaries;
        }

        // No rows means either "no such function" or "a function with zero boundaries" — the two need
        // different handling, so ask directly.
        await using var exists = conn.CreateCommand();
        exists.CommandText = "SELECT COUNT(*) FROM sys.partition_functions WHERE name = @pf";
        exists.Parameters.AddWithValue("@pf", pfName);

        var count = (int?)await exists.ExecuteScalarAsync(token).ConfigureAwait(false) ?? 0;

        return count > 0 ? boundaries : null;
    }

    private static DateTimeOffset? AsMoment(object? value)
    {
        return value switch
        {
            DateTime dt => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc), TimeSpan.Zero),
            DateTimeOffset dto => dto.ToUniversalTime(),
            _ => null
        };
    }

    private sealed record ActualBoundary(string Literal, DateTimeOffset? Moment);
}
