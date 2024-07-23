using JasperFx.Core;

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
}