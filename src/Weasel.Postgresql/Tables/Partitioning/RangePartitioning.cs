using System.Data.Common;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

public class RangePartitioning: IPartitionStrategy
{
    private readonly List<RangePartition> _ranges = new();

    public IReadOnlyList<RangePartition> Ranges => _ranges;

    /// <summary>
    /// The database columns to use as part of the hashing strategy
    /// </summary>
    public string[] Columns { get; init; }

    public bool HasExistingDefault { get; private set; }

    /// <summary>
    /// The runtime partition manager that owns this table's partition set, if any. Set through
    /// <see cref="UsePartitionManager"/>.
    /// </summary>
    public IRangePartitionManager? PartitionManager { get; private set; }

    /// <summary>
    /// Hand ownership of the partition set to a runtime manager — typically a
    /// <see cref="ManagedRangePartitions"/> rolling time window — instead of declaring a static list of
    /// ranges. The manager recomputes the expected partitions on every read, and delta detection becomes
    /// purely additive so that a window rolling forward never triggers a table rebuild.
    /// </summary>
    public RangePartitioning UsePartitionManager(IRangePartitionManager manager)
    {
        PartitionManager = manager ?? throw new ArgumentNullException(nameof(manager));
        return this;
    }

    /// <summary>
    /// The partitions this strategy currently expects: the manager's set when one is attached, otherwise
    /// the statically declared ranges.
    /// </summary>
    private IReadOnlyList<RangePartition> expectedRanges()
        => PartitionManager == null ? _ranges : PartitionManager.Partitions().ToList();

    void IPartitionStrategy.WritePartitionBy(TextWriter writer)
    {
        writer.WriteLine($") PARTITION BY RANGE ({Columns.Join(", ")});");
    }

    PartitionDelta IPartitionStrategy.CreateDelta(Table parent, IPartitionStrategy actual, out IPartition[] missing)
    {
        missing = default;
        if (actual is RangePartitioning other)
        {
            if (!Columns.SequenceEqual(other.Columns))
            {
                return PartitionDelta.Rebuild;
            }

            if (parent.IgnorePartitionsInMigration) return PartitionDelta.None;

            var expected = expectedRanges();

            var match = expected.OrderBy(x => x.Suffix).ToArray()
                .SequenceEqual(other._ranges.OrderBy(x => x.Suffix).ToArray());

            if (match) return PartitionDelta.None;

            if (PartitionManager != null)
            {
                // weasel#401: a managed rolling window OWNS its partition set, and that set is a function
                // of the clock. Every time "now" crosses a period boundary the declared window gains a
                // partition at the leading edge and loses one at the trailing edge, so the two conditions
                // the declarative branch below reads as drift — the actual database has partitions the
                // declaration no longer names, and it has more of them than we asked for — are the normal
                // steady state of a time-series table, not an anomaly. Rebuilding a multi-gigabyte table
                // because last month rolled off would be catastrophic. Aged partitions are retired by the
                // manager's retention pass (ManagedRangePartitions.DropAgedPartitionsAsync), which is a
                // policy outcome rather than a migration, so migration only ever ADDS here.
                //
                // Match on the suffix rather than on full equality. The suffix already determines the
                // bounds for a given policy, and the create path is CREATE TABLE IF NOT EXISTS keyed on
                // the partition's NAME — so a partition reported as missing while a table of that name
                // already exists would be silently skipped and then reported missing again on the next
                // migration, forever. Matching on the name the create path actually uses keeps migration
                // convergent.
                var actualSuffixes = other._ranges.Select(x => x.Suffix).ToHashSet(StringComparer.OrdinalIgnoreCase);

                missing = expected.Where(x => !actualSuffixes.Contains(x.Suffix)).OfType<IPartition>().ToArray();
                return missing.Length != 0 ? PartitionDelta.Additive : PartitionDelta.None;
            }

            // We've already done a SequenceEqual, so we know the counts aren't the same
            // and if there are more actual partitions than expected, we need to do a rebalance
            if (other._ranges.Count > _ranges.Count) return PartitionDelta.Rebuild;

            // If any partitions are in the actual that are no longer expected, that's an automatic rebuild
            if (other._ranges.Any(x => !_ranges.Contains(x))) return PartitionDelta.Rebuild;

            missing = _ranges.Where(x => !other._ranges.Contains(x)).OfType<IPartition>().ToArray();
            return missing.Length != 0 ? PartitionDelta.Additive : PartitionDelta.Rebuild;
        }
        else
        {
            return PartitionDelta.Rebuild;
        }
    }

    public IEnumerable<string> PartitionTableNames(Table parent)
    {
        foreach (var partition in expectedRanges())
        {
            yield return $"{parent.Identifier.Name.ToLowerInvariant()}_{partition.Suffix.ToLowerInvariant()}";
        }

        yield return $"{parent.Identifier.Name.ToLowerInvariant()}_default";
    }

    /// <summary>
    /// Add another range partition with the name "{parent table name}_{suffix}"
    /// </summary>
    /// <param name="suffix">The suffix for the partition table name</param>
    /// <param name="from">The "from" value</param>
    /// <param name="to">The "to" value</param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public RangePartitioning AddRange<T>(string suffix, T from, T to)
    {
        if (PartitionManager != null)
        {
            throw new InvalidOperationException(
                "This table's partitions are owned by a partition manager, so statically declared ranges would be silently ignored. Remove the UsePartitionManager() call or the AddRange() call.");
        }

        var partition = new RangePartition(suffix, from.FormatSqlValue(), to.FormatSqlValue());
        _ranges.Add(partition);

        return this;
    }

    /// <summary>
    /// Add a range whose bounds are <b>already</b> formatted SQL literals — the shape PostgreSQL echoes back
    /// from <c>pg_get_expr(relpartbound)</c>, where every bound comes back single-quoted. Use this instead of
    /// <see cref="AddRange{T}"/> when the bounds have already been through
    /// <see cref="PartitionExtensions.FormatSqlValue{T}"/> or came out of the catalog.
    ///
    /// <para>
    /// <see cref="AddRange{T}"/> formats raw values and is deliberately not idempotent, so passing it a
    /// literal would escape the quotes a second time. This used to work by accident: FormatSqlValue
    /// short-circuited on any string that started and ended with a quote, which also meant a quote-wrapped
    /// value skipped escaping entirely. See weasel#416.
    /// </para>
    /// </summary>
    internal RangePartitioning AddRangeWithSqlLiterals(string suffix, string fromLiteral, string toLiteral)
    {
        _ranges.Add(new RangePartition(suffix, fromLiteral, toLiteral));

        return this;
    }

    void IPartitionStrategy.WriteCreateStatement(TextWriter writer, Table parent)
    {
        foreach (IPartition partition in expectedRanges())
        {
            partition.WriteCreateStatement(writer, parent);
            writer.WriteLine();
        }

        writer.WriteDefaultPartition(parent.Identifier);
    }

    internal async Task ReadPartitionsAsync(DbObjectName identifier, DbDataReader reader, CancellationToken ct)
    {
        var expectedDefaultName = identifier.Name + "_default";
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var partitionName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            var expression = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);

            if (partitionName == expectedDefaultName)
            {
                HasExistingDefault = true;
            }
            else
            {
                var range = RangePartition.Parse(identifier, partitionName, expression);
                _ranges.Add(range);
            }
        }
    }
}
