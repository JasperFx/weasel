namespace Weasel.Postgresql.Tables.Partitioning;

public interface IPartition
{
    void WriteCreateStatement(TextWriter writer, Table parent);
}
