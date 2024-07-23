namespace Weasel.Postgresql.Tables.Partitioning;

public class RangePartition
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
}