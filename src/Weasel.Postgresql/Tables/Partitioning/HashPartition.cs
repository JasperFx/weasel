namespace Weasel.Postgresql.Tables.Partitioning;

using Weasel.Core;
using Weasel.Postgresql;

public record HashPartition
{
    public HashPartition(string suffix, int modulus, int remainder)
    {
        Suffix = suffix.ToLowerInvariant();
        Modulus = modulus;
        Remainder = remainder;
    }

    public int Remainder { get; }
    public int Modulus { get; }
    public string Suffix { get; }

    public void WriteCreateStatement(TextWriter writer, Table parent)
    {
        var partitionName = PostgresqlObjectName.From(
            new DbObjectName(parent.Identifier.Schema, parent.Identifier.Name + "_" + Suffix));
        var parentName = PostgresqlObjectName.From(parent.Identifier);
        writer.WriteLine($"create table {partitionName} partition of {parentName} for values with (modulus {Modulus}, remainder {Remainder});");
    }

    public static HashPartition Parse(string suffix, string expression)
    {
        // FOR VALUES WITH (modulus 3, remainder 0)
        var span = expression.GetSpanWithinParentheses();
        Span<Range> ranges = stackalloc Range[2];
        span.Split(ranges, ',', StringSplitOptions.TrimEntries);
        var modulus = int.Parse(span[ranges[0]][7..]);
        var remainder = int.Parse(span[ranges[1]][9..]);
        return new HashPartition(suffix, modulus, remainder);
    }

    public virtual bool Equals(HashPartition? other)
    {
        if (ReferenceEquals(null, other))
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return Suffix == other.Suffix && Modulus == other.Modulus && Remainder == other.Remainder;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Suffix, Modulus, Remainder);
    }

    public override string ToString()
    {
        return $"{nameof(Suffix)}: {Suffix}, {nameof(Modulus)}: {Modulus}, {nameof(Remainder)}: {Remainder}";
    }
}
