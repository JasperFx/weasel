using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

public class ListPartition : IPartition
{
    public ListPartition(string suffix, params string[] values)
    {
        Suffix = suffix;
        Values = values;
    }

    /// <summary>
    /// The SQL representation of the values used to create this list partition
    /// </summary>
    public string[] Values { get; }

    /// <summary>
    /// Table suffix for this partition
    /// </summary>
    public string Suffix { get; }

    void IPartition.WriteCreateStatement(TextWriter writer, Table parent)
    {
        writer.WriteLine($"CREATE TABLE {parent.Identifier}_{Suffix} partition of {parent.Identifier} for values in ({Values.Join(", ")});");
    }

    internal static ListPartition Parse(DbObjectName dbObjectName, string partitionTableName, string postgresExpression)
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
