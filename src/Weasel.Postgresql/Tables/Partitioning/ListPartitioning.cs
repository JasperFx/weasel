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

    public ListPartitioning AddPartition<T>(string suffix, params T[] values)
    {
        var partition = new ListPartition(suffix, values.Select(x => x.FormatSqlValue()).ToArray());
        _partitions.Add(partition);

        return this;
    }

    public void WriteCreateStatement(TextWriter writer, Table parent)
    {
        foreach (var partition in _partitions)
        {
            partition.WriteCreateStatement(writer, parent);
            writer.WriteLine();
        }

        writer.WriteDefaultPartition(parent.Identifier);
    }

    public void WritePartitionBy(TextWriter writer)
    {
        writer.WriteLine($") PARTITION BY LIST ({Columns.Join(", ")});");
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
                var partition = ListPartition.Parse(identifier, partitionName, expression);
                _partitions.Add(partition);
            }
        }
    }

    public bool HasExistingDefault { get; private set; }
}
