namespace Weasel.Postgresql.SqlGeneration;

// TODO -- move to Weasel
public interface IWhereFragmentHolder
{
    void Register(ISqlFragment fragment);
}
