using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

public class ListPartition
{
    public ListPartition(string suffix, params string[] values)
    {
        Suffix = suffix;
        Values = values;
    }

    public string[] Values { get; }
    public string Suffix { get; }

    public void WriteCreateStatement(TextWriter writer, Table parent)
    {
        writer.WriteLine($"CREATE TABLE {parent.Identifier}_{Suffix} partition of {parent.Identifier} for values in ({Values.Join(", ")});");
    }

    public static ListPartition Parse(DbObjectName dbObjectName, string partitionTableName, string postgresExpression)
    {
        var suffix = dbObjectName.GetSuffixName(partitionTableName);
        var parsed = postgresExpression.GetStringWithinParantheses().ToDelimitedArray(',');

        return new ListPartition(suffix, parsed);
    }

    protected bool Equals(ListPartition other)
    {
        return Values.SequenceEqual(other.Values) && Suffix == other.Suffix;
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

        return Equals((ListPartition)obj);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Values, Suffix);
    }

    public override string ToString()
    {
        return $"{nameof(Values)}: {Values.Join(", ")}, {nameof(Suffix)}: {Suffix}";
    }
}
