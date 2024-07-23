namespace Weasel.Postgresql.Tables.Partitioning;

public interface IPartitionStrategy
{
    void WriteCreateStatement(TextWriter writer, Table parent);
    string[] Columns { get; }

    void WritePartitionBy(TextWriter writer);
}