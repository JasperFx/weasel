using System.Text;
using JasperFx.Core;
using Weasel.Core;
using Weasel.Postgresql;

namespace Weasel.Postgresql.Tables.Partitioning;

public class ListPartition : IPartition
{
    public ListPartition(string suffix, params string[] values)
    {
        Suffix = SanitizeSuffix(suffix);
        Values = values;
    }

    /// <summary>
    /// A partition table is named <c>{parent}_{suffix}</c> as an UNQUOTED Postgres identifier, so the
    /// suffix may only contain identifier-safe characters. Tenant ids are frequently used directly as
    /// the suffix and can contain '-' (and other non-identifier characters), which produced invalid DDL
    /// (<c>CREATE TABLE ..._tenant-a ...</c> → <c>42601 syntax error at or near "-"</c>). Lower-case and
    /// replace any character outside <c>[a-z0-9_]</c> with '_'. The partition VALUES keep the exact
    /// tenant id, so partition routing is unaffected — only the internal table name is normalized.
    /// Idempotent, so the <see cref="Parse"/> round-trip (which re-reads the suffix from the table name)
    /// stays symmetric. NOTE: distinct suffixes that normalize to the same string (e.g. <c>a-b</c> and
    /// <c>a_b</c>) collide on the same partition table; callers needing both must supply already-distinct
    /// suffixes.
    /// </summary>
    internal static string SanitizeSuffix(string suffix)
    {
        var lower = suffix.ToLowerInvariant();
        var builder = new StringBuilder(lower.Length);
        foreach (var c in lower)
        {
            builder.Append((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' ? c : '_');
        }

        return builder.ToString();
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
        var partitionName = PostgresqlObjectName.From(
            new DbObjectName(parent.Identifier.Schema, parent.Identifier.Name + "_" + Suffix));
        var parentName = PostgresqlObjectName.From(parent.Identifier);
        // IF NOT EXISTS: concurrent schema appliers (e.g. tenant provisioning racing an async daemon's
        // storage check) may both compute the same missing partition; creation must be idempotent.
        writer.WriteLine($"CREATE TABLE IF NOT EXISTS {partitionName} partition of {parentName} for values in ({Values.Join(", ")});");
    }

    internal static ListPartition Parse(DbObjectName dbObjectName, string partitionTableName, string postgresExpression)
    {
        var suffix = dbObjectName.GetSuffixName(partitionTableName);
        var parsed = postgresExpression.GetStringWithinParantheses().ToDelimitedArray(',');

        return new ListPartition(suffix, parsed);
    }

    protected bool Equals(ListPartition other)
    {
        // Values hold raw SQL literals that differ between the declared side and what PostgreSQL
        // echoes back (it single-quotes every literal, so a declared int 20 comes back as '20'), so
        // compare on the normalized form to avoid spurious migration rebuilds. String values already
        // match because FormatSqlValue quotes them on both sides; integer and date values would drift.
        // See PartitionExtensions.NormalizePartitionValue (mirrors the RangePartition fix, weasel#318).
        if (Values.Length != other.Values.Length) return false;
        for (int i = 0; i < Values.Length; i++)
        {
            if (Values[i].NormalizePartitionValue() != other.Values[i].NormalizePartitionValue())
            {
                return false;
            }
        }

        return Suffix == other.Suffix;
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
        // Combine the normalized element values (not the array reference) + Suffix so the hash is
        // consistent with Equals and stable across the declared-vs-readback quoting difference.
        var hash = new HashCode();
        foreach (var value in Values)
        {
            hash.Add(value.NormalizePartitionValue());
        }

        hash.Add(Suffix);
        return hash.ToHashCode();
    }

    public override string ToString()
    {
        return $"{nameof(Values)}: {Values.Join(", ")}, {nameof(Suffix)}: {Suffix}";
    }
}
