using JasperFx.Core;
using JasperFx.Core.Reflection;

namespace Weasel.Postgresql.Tables;


public enum PartitionStrategy
{
    /// <summary>
    ///     No partitioning
    /// </summary>
    None,

    /// <summary>
    ///     Postgresql PARTITION BY RANGE semantics
    /// </summary>
    Range,

    /// <summary>
    /// PARTITION BY LIST semantics
    /// </summary>
    List,

    /// <summary>
    /// PARTITION BY HASH semantics
    /// </summary>
    Hash
}


public interface IPartition
{
    string Suffix { get; }
    void WriteCreateStatement(TextWriter writer, Table parent);
}

public class DefaultPartition: IPartition
{
    public string Suffix => "default";
    public void WriteCreateStatement(TextWriter writer, Table parent)
    {
        writer.WriteLine($"CREATE TABLE {parent.Identifier}_default PARTITION OF {parent.Identifier} DEFAULT;");
    }
}

public class ListPartition<T>: IPartition
{
    public ListPartition(string suffix, params T[] values)
    {
        Suffix = suffix;
        Values = values;
    }

    public T[] Values { get; }
    public string Suffix { get; }

    public void WriteCreateStatement(TextWriter writer, Table parent)
    {
        var values = Values.Select(x => x.ToString()).ToArray();
        if (typeof(T) == typeof(string))
        {
            values = values.Select(x => $"'{x}'").ToArray();
        }

        writer.WriteLine($"CREATE TABLE {parent.Identifier}_{Suffix} partition of {parent.Identifier} for values in ({values.Join(", ")});");
    }

    public PartitionStrategy Strategy { get; }
}

public class HashPartitioning
{
    /// <summary>
    /// The database columns to use as part of the hashing strategy
    /// </summary>
    public string[] Columns { get; init; }

    /// <summary>
    /// The suffix names for the partitioned table names. The modulo/remainder values
    /// will be created automatically based on the number of suffixes
    /// </summary>
    public string[] Suffixes { get; init; }

    internal IEnumerable<IPartition> BuildPartitions()
    {
        var modulus = Suffixes.Length;
        var remainder = 0;
        foreach (var suffix in Suffixes)
        {
            yield return new HashPartition(suffix, modulus, remainder);
            remainder++;
        }
    }
}

public record HashPartition(string Suffix, int Modulus, int Remainder): IPartition
{
    public void WriteCreateStatement(TextWriter writer, Table parent)
    {
        writer.WriteLine($"create table {parent.Identifier}_{Suffix} partition of {parent.Identifier} for values with (modulus {Modulus}, remainder {Remainder});");
    }
}

public class RangePartition<T>: IPartition
{
    public string Suffix { get; }
    public T From { get; }
    public T To { get; }

    public RangePartition(string suffix, T from, T to)
    {
        Suffix = suffix;
        From = from;
        To = to;
    }

    public void WriteCreateStatement(TextWriter writer, Table parent)
    {
        var from = typeof(T).IsNumeric() ? From.ToString() : $"'{From.ToString()}'";
        var to = typeof(T).IsNumeric() ? To.ToString() : $"'{To.ToString()}'";

        writer.WriteLine($"CREATE TABLE {parent.Identifier}_{Suffix} PARTITION OF {parent.Identifier} FOR VALUES FROM ({from}) TO ({to});");
    }
}
