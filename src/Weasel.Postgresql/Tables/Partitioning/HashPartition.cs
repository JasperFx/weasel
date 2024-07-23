namespace Weasel.Postgresql.Tables.Partitioning;

public record HashPartition(string Suffix, int Modulus, int Remainder)
{
    public void WriteCreateStatement(TextWriter writer, Table parent)
    {
        writer.WriteLine($"create table {parent.Identifier}_{Suffix} partition of {parent.Identifier} for values with (modulus {Modulus}, remainder {Remainder});");
    }
}