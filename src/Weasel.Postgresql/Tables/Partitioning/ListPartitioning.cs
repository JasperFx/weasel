using System.Data.Common;
using System.Diagnostics;
using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

public class ListPartitioning: IPartitionStrategy
{
    private readonly List<ListPartition> _partitions = new();
    public string[] Columns { get; init; }

    public IReadOnlyList<ListPartition> Partitions => _partitions;

    /// <summary>
    /// </summary>
    public bool EnableDefaultPartition { get; set; } = true;

    public IListPartitionManager? PartitionManager { get; private set; }

    /// <summary>
    /// Apply a list partition manager that will control the exact partitions
    /// </summary>
    /// <param name="strategy"></param>
    /// <returns></returns>
    public ListPartitioning UsePartitionManager(IListPartitionManager strategy)
    {
        EnableDefaultPartition = false;
        PartitionManager = strategy;

        return this;
    }

    /// <summary>
    /// The partitions this strategy currently expects: the manager's set when one is attached,
    /// otherwise the statically declared ones.
    /// </summary>
    /// <remarks>
    /// Every call site resolves through here, the way <c>RangePartitioning.expectedRanges</c> does.
    /// PartitionTableNames used to read <c>_partitions</c> directly, so for a manager-owned
    /// partitioning -- where <see cref="UsePartitionManager"/> also clears
    /// <see cref="EnableDefaultPartition"/> -- it returned the EMPTY sequence rather than a short
    /// one, and a concurrent index over the table rendered only its first step and stayed invalid
    /// forever (weasel#520).
    /// <para>
    /// A manager reads its partitions from a lookup table, so this is a point-in-time answer. That
    /// is correct for a migration -- a partition created afterwards inherits the parent's indexes
    /// automatically -- but any DDL built from it is only as complete as the manager's state at the
    /// moment it was rendered.
    /// </para>
    /// </remarks>
    private IReadOnlyList<ListPartition> expectedPartitions()
        => PartitionManager == null ? _partitions : PartitionManager.Partitions().ToList();

    /// <summary>
    /// Add another list partition table based on the supplied table suffix and values
    /// </summary>
    /// <param name="suffix"></param>
    /// <param name="values"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public ListPartitioning AddPartition<T>(string suffix, params T[] values)
    {
        var partition = new ListPartition(suffix, values.Select(x => x.FormatSqlValue()).ToArray());
        _partitions.Add(partition);

        return this;
    }

    /// <summary>
    /// Add a partition whose values are <b>already</b> formatted SQL literals — the shape PostgreSQL echoes
    /// back from <c>pg_get_expr(relpartbound)</c>, where every value comes back single-quoted. Use this
    /// instead of <see cref="AddPartition{T}"/> when the values have already been through
    /// <see cref="PartitionExtensions.FormatSqlValue{T}"/> or came out of the catalog.
    ///
    /// <para>
    /// <see cref="AddPartition{T}"/> formats raw values and is deliberately not idempotent, so passing it a
    /// literal would escape the quotes a second time. This used to work by accident: FormatSqlValue
    /// short-circuited on any string that started and ended with a quote, which also meant a quote-wrapped
    /// value skipped escaping entirely. See weasel#416.
    /// </para>
    /// </summary>
    internal ListPartitioning AddPartitionWithSqlLiterals(string suffix, params string[] sqlLiterals)
    {
        _partitions.Add(new ListPartition(suffix, sqlLiterals));

        return this;
    }

    void IPartitionStrategy.WriteCreateStatement(TextWriter writer, Table parent)
    {
        var partitions = expectedPartitions();

        foreach (IPartition partition in partitions)
        {
            partition.WriteCreateStatement(writer, parent);
            writer.WriteLine();
        }

        if (EnableDefaultPartition)
        {
            writer.WriteDefaultPartition(parent.Identifier);
        }
    }

    void IPartitionStrategy.WritePartitionBy(TextWriter writer)
    {
        writer.WriteLine($") PARTITION BY LIST ({Columns.Join(", ")});");
    }

    PartitionDelta IPartitionStrategy.CreateDelta(Table parent, IPartitionStrategy actual, out IPartition[] missing)
    {
        missing = default;
        if (actual is ListPartitioning other)
        {
            var partitions = expectedPartitions();

            if (!Columns.SequenceEqual(other.Columns))
            {
                return PartitionDelta.Rebuild;
            }

            if (parent.IgnorePartitionsInMigration) return PartitionDelta.None;

            var match = partitions.OrderBy(x => x.Suffix).ToArray()
                .SequenceEqual(other.Partitions.OrderBy(x => x.Suffix).ToArray());

            if (match) return PartitionDelta.None;

            // We've already done a SequenceEqual, so we know the counts aren't the same
            // and if there are more actual partitions than expected, we need to do a rebalance
            if (other.Partitions.Count > partitions.Count) return PartitionDelta.Rebuild;

            // If any partitions are in the actual that are no longer expected, that's an automatic rebuild
            if (other._partitions.Any(x => !partitions.Contains(x))) return PartitionDelta.Rebuild;

            missing = partitions.Where(x => !other._partitions.Contains(x)).OfType<IPartition>().ToArray();
            return missing.Any() ? PartitionDelta.Additive : PartitionDelta.Rebuild;
        }
        else
        {
            return PartitionDelta.Rebuild;
        }
    }

    public IEnumerable<string> PartitionTableNames(Table parent)
    {
        foreach (var partition in expectedPartitions())
        {
            yield return $"{parent.Identifier.Name.ToLowerInvariant()}_{partition.Suffix.ToLowerInvariant()}";
        }

        if (EnableDefaultPartition)
        {
            yield return $"{parent.Identifier.Name.ToLowerInvariant()}_default";
        }
    }

    public async Task ReadPartitionsAsync(DbObjectName identifier, DbDataReader reader, CancellationToken ct)
    {
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var partitionName = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            var expression = await reader.GetFieldValueAsync<string>(1, ct).ConfigureAwait(false);

            // Classify by the partition's bound EXPRESSION, not its table name. PostgreSQL reports
            // pg_get_expr(relpartbound, ...) == "DEFAULT" for the default partition. A regular value
            // partition that merely happens to be named "<table>_default" — e.g. a managed list
            // partition whose suffix is "default", such as Marten's *DEFAULT* tenant — reports
            // "FOR VALUES IN (...)" and must be parsed as a normal partition. Classifying by name
            // mistook it for the default partition and dropped it from the actual set, so CreateDelta
            // perpetually reported it as missing and re-issued CREATE TABLE ..._default, failing the
            // next migration with 42P07 "relation already exists".
            if (expression.Trim() == "DEFAULT")
            {
                HasExistingDefault = true;
            }
            else
            {
                var partition = ListPartition.Parse(identifier, partitionName, expression);
                _partitions.Add(partition);
            }
        }
    }

    public bool HasExistingDefault { get; private set; }

    /// <summary>
    /// Disable the default partition
    /// </summary>
    /// <returns></returns>
    public ListPartitioning DisableDefaultPartition()
    {
        EnableDefaultPartition = false;
        return this;
    }
}
