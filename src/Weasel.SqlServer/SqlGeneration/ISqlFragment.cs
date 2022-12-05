namespace Weasel.SqlServer.SqlGeneration;

public interface ISqlFragment
{
    void Apply(CommandBuilder builder);

    bool Contains(string sqlText);
}
