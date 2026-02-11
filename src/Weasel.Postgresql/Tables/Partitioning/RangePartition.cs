using System.Diagnostics;
using Weasel.Core;
using Weasel.Postgresql;

namespace Weasel.Postgresql.Tables.Partitioning;

public class RangePartition : IPartition
{
    /// <summary>
    /// Table suffix of this partitioned table
    /// </summary>
    public string Suffix { get; }

    /// <summary>
    /// The SQL "from" value of the partition
    /// </summary>
    public string From { get; }

    /// <summary>
    /// The SQL "to" value of the partition
    /// </summary>
    public string To { get; }

    public RangePartition(string suffix, string from, string to)
    {
        Suffix = suffix.ToLowerInvariant();
        From = from;
        To = to;
    }

    void IPartition.WriteCreateStatement(TextWriter writer, Table parent)
    {
        var partitionName = PostgresqlObjectName.From(
            new DbObjectName(parent.Identifier.Schema, parent.Identifier.Name + "_" + Suffix));
        var parentName = PostgresqlObjectName.From(parent.Identifier);
        writer.WriteLine($"CREATE TABLE {partitionName} PARTITION OF {parentName} FOR VALUES FROM ({From}) TO ({To});");
    }

    protected bool Equals(RangePartition other)
    {
        return Suffix == other.Suffix && From == other.From && To == other.To;
    }

    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(null, obj))
        {
            return false;
        }

        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        if (obj.GetType() != this.GetType())
        {
            return false;
        }

        return Equals((RangePartition)obj);
    }

    public override string ToString()
    {
        return $"{nameof(Suffix)}: {Suffix}, {nameof(From)}: {From}, {nameof(To)}: {To}";
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Suffix, From, To);
    }

    internal static RangePartition Parse(DbObjectName tableName, string partitionName, string postgresExpression)
    {
        var suffix = tableName.GetSuffixName(partitionName);

        var parts = postgresExpression.Split("TO");
        var from = parts[0].GetStringWithinParantheses();
        var to = parts[1].GetStringWithinParantheses();

        return new RangePartition(suffix, from, to);
    }
}
