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

    public void WritePartitionBy(TextWriter writer)
    {
        writer.WriteLine($") PARTITION BY RANGE ({Columns.Join(", ")});");
    }

    public RangePartitioning AddRange<T>(string suffix, T from, T to)
    {
        var partition = new RangePartition(suffix, from.FormatSqlValue(), to.FormatSqlValue());
        _ranges.Add(partition);

        return this;
    }

    public void WriteCreateStatement(TextWriter writer, Table parent)
    {
        foreach (var partition in _ranges)
        {
            partition.WriteCreateStatement(writer, parent);
            writer.WriteLine();
        }

        writer.WriteDefaultPartition(parent.Identifier);
    }

    public async Task ReadPartitionsAsync(DbObjectName identifier, DbDataReader reader, CancellationToken ct)
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
