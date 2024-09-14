using JasperFx.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

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
        writer.WriteLine($"create table {parent.Identifier}_{Suffix} partition of {parent.Identifier} for values with (modulus {Modulus}, remainder {Remainder});");
    }

    public static HashPartition Parse(string suffix, string expression)
    {
        // FOR VALUES WITH (modulus 3, remainder 0)
        var parts = expression.GetStringWithinParantheses().ToDelimitedArray();
        var modulus = int.Parse(parts[0].Substring(7).Trim());
        var remainder = int.Parse(parts[1].Substring(9).Trim());

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
