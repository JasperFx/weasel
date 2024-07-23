using System.Diagnostics;
using Weasel.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

public class RangePartition : IPartition
{
    public string Suffix { get; }
    public string From { get; }
    public string To { get; }

    public RangePartition(string suffix, string from, string to)
    {
        Suffix = suffix;
        From = from;
        To = to;
    }

    public void WriteCreateStatement(TextWriter writer, Table parent)
    {
        writer.WriteLine($"CREATE TABLE {parent.Identifier}_{Suffix} PARTITION OF {parent.Identifier} FOR VALUES FROM ({From}) TO ({To});");
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

    public static RangePartition Parse(DbObjectName tableName, string partitionName, string postgresExpression)
    {
        var suffix = tableName.GetSuffixName(partitionName);

        var parts = postgresExpression.Split("TO");
        var from = parts[0].GetStringWithinParantheses();
        var to = parts[1].GetStringWithinParantheses();

        return new RangePartition(suffix, from, to);
    }
}
